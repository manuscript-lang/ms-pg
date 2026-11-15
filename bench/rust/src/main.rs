// Query throughput: WORKERS connections each run QUERIES sequential point lookups with one
// prepared statement. Matches ms/src/main.ms.
use tokio_postgres::{Config, NoTls};

fn env(name: &str, fallback: &str) -> String {
    std::env::var(name).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| fallback.to_string())
}

async fn worker(config: Config, id: i64, queries: i64) -> Result<i64, tokio_postgres::Error> {
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(connection);
    let statement = client.prepare("select id, name, score from items where id = $1").await?;
    let mut sum = 0;
    for i in 0..queries {
        let key = (i * 7919 + id * 104729) % 10000 + 1;
        let row = client.query_one(&statement, &[&key]).await?;
        let _id: i64 = row.get(0);
        let _name: String = row.get(1);
        let score: i64 = row.get(2);
        sum += score;
    }
    Ok(sum)
}

/// A timestamptz read as raw microseconds since 2000-01-01, so no date crate is needed.
struct Micros(i64);

impl<'a> tokio_postgres::types::FromSql<'a> for Micros {
    fn from_sql(_: &tokio_postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Micros(i64::from_be_bytes(raw.try_into()?)))
    }
    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::TIMESTAMPTZ
    }
}

/// Reads 1000 consecutive rows of the readings table per query and sums every column.
/// Matches numbers_worker in ms/src/main.ms.
async fn numbers_worker(config: Config, id: i64, queries: i64) -> Result<i64, tokio_postgres::Error> {
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(connection);
    let statement = client.prepare("select id, a, b, c, ts from readings where id between $1 and $2").await?;
    let mut sum = 0;
    for i in 0..queries {
        let low = (i * 7919 + id * 104729) % 99000 + 1;
        let high = low + 999;
        for row in client.query(&statement, &[&low, &high]).await? {
            let row_id: i64 = row.get(0);
            let a: i32 = row.get(1);
            let b: f64 = row.get(2);
            let c: f32 = row.get(3);
            let Micros(micros) = row.get(4);
            let unix = micros + 946_684_800_000_000;
            sum += row_id + a as i64 + (b * 2.0) as i64 + (c * 4.0) as i64
                + unix.div_euclid(1_000_000)
                + unix.rem_euclid(1_000_000);
        }
    }
    Ok(sum)
}

#[tokio::main]
async fn main() {
    let workers: i64 = env("WORKERS", "4").parse().unwrap();
    let queries: i64 = env("QUERIES", "20000").parse().unwrap();
    let mut config = Config::new();
    config
        .host(&env("PGHOST", "127.0.0.1"))
        .port(env("PGPORT", "5432").parse().unwrap())
        .user(&env("PGUSER", "postgres"))
        .password(env("PGPASSWORD", ""))
        .dbname("postgres");
    let numbers = env("BENCH", "lookup") == "numbers";
    let tasks: Vec<_> = (0..workers)
        .map(|id| {
            if numbers {
                tokio::spawn(numbers_worker(config.clone(), id, queries))
            } else {
                tokio::spawn(worker(config.clone(), id, queries))
            }
        })
        .collect();
    let mut total = 0;
    for task in tasks {
        total += task.await.unwrap().unwrap();
    }
    println!("checksum {total}");
}
