# ms-pg

A PostgreSQL client written in Manuscript over the standard library's TCP and TLS. No C and no Rust crates.

Supported: protocol v3 startup, SCRAM-SHA-256 login (the server must prove it knows the password), the simple and
extended query protocols (parse, bind and execute in one round trip), prepared statements, typed parameters, results in text or binary format, transactions, server errors that carry their SQLSTATE in `Error.kind`, and a pool of worker tasks.

## Binary results

Prepared statements (`Conn.prepare` then `Conn.run`) ask for each result column in binary format when the client has a
decoder for its type: `int2`, `int4`, `int8`, `oid`, `float4`, `float8`, `bool`, `bytea`, `text`, `varchar`, `uuid`,
`date`, `timestamp` and `timestamptz`. Every other type, such as `numeric`, stays text, chosen per column in the Bind
message. Parameters are still sent as text, and `Conn.query` (one round trip, columns unknown until it returns) stays
text. Set `Config.binary_results` to false for text everywhere.

## Typed values

The format changes speed, never the type: `Conn.query`, `Conn.simple`, `Conn.run` and the pool all return the same
`Value` variant for a column, in text or binary. A `timestamp` or `timestamptz` is `Value.Timestamp` (UTC; a `timestamp`
without a zone is read as UTC), a `date` is `Value.Date` (days since 1970-01-01), a `uuid` is `Value.Text`, and
`infinity` values are `Value.Text("infinity")` and `Value.Text("-infinity")`. A `float4` widens exactly to a `Float`, so
`0.1::float4` is 0.10000000149011612 in both formats. Read values with `value.int()`, `float()`, `bool()`, `text()`,
`bytes()`, `timestamp()`, `date()` (each `none` for another variant) and `is_null()`.

## Pipelines

Two ways to get rows as a `ReadStream`, so they compose with the standard stream steps:

```manuscript
import stream from "@pg/stream.ms"
import collect, filter, map from "@std/io/streams.ms"

// Rows as the server sends them, on their own connection; a slow reader slows the server.
big := stream(config, "select id, name from items") | filter(fn row: row[0].int().get(0) > 100) | map(fn row: row[1].text().get(""))
names := collect(big, max_items: 1000)?

// Or the rows of a result you already fetched with `conn.query`:
evens := collect(rows.stream() | filter(fn row: row[0].int().get(0) % 2 == 0), max_items: 100)?
```

On a pooled connection, `pool.stream(sql, params)` returns a real `ReadStream` of rows. The caller gets a pipe of its own and the worker keeps its connection, so there is no extra login and streams from different calls do not share a reader:

```manuscript
var source = pool.stream("select n from big")?
evens := collect(source | filter(fn row: row[0].int().get(0) % 2 == 0), max_items: 100)?
```

A server error is the stream's last item. A stream that is not read holds its worker through back pressure: read it to the end or close it before asking the same worker for more, and close every stream before `pool.close()`. `pool.each(sql, params, fn row: ...)` is the callback form of the same thing.

This needs `Pipe.ticket()` from the standard library (manuscript-lang/manuscript#4132): the request carries a plain-data ticket for the caller's pipe, and the worker opens it.

`stream` runs the query in a task with its own connection (a task cannot borrow yours), so each stream pays one login. A server or login error is the stream's last item; closing the stream early (`take`, `break`) stops the query.

## Logins

Only SCRAM-SHA-256 is accepted by default. A cleartext password is sent only over TLS. An md5 challenge is refused
unless `Config.allow_md5` is true.

## TLS

Set `Config.tls` to `TlsOptions(server_name: ..., roots_pem: ...)`. The client sends an SSLRequest, accepts only a single
`S` byte as the answer, then upgrades the same socket with `start_tls` and verifies the server certificate. A server
that answers `N` is an error, never a plaintext fallback. `server_name` defaults to `host`. Works with every server
version that supports TLS.

## Use

```manuscript
import Config, open from "@pg/conn.ms"
import Value from "@pg/value.ms"

fn main() Void!:
    var conn = open(Config(password: "secret"))?
    rows := conn.query("select $1::int8 + 1", [Value.Int(41)])?
    print("{rows.rows[0][0]}")
    conn.close()?
    return ok()
```

`Pool` (`@pg/pool.ms`) runs statements on worker tasks that each own one connection. A connection cannot move
between tasks, so statements travel to the workers over pipes and rows travel back; one task owns the `Pool`. `query_all` queues at most 32 statements per worker before reading replies, so any number of statements is safe. Pool statements use the text format.

## Tests

`eval $(tests/start-postgres.sh /tmp/pg)` starts a throwaway server with the roles the tests need, then `ms test`.
Settings come from `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and `PGDATABASE`. A workflow runs them every night
and on dispatch on Linux, macOS and Windows, with a `ms` built from the `manuscript` repository.

## As a dependency

This repository is a Manuscript package: its root holds `ms.toml` and `src/`. Add it to a project with `ms add`, which
installs it, pins it in `ms.lock`, and with `--allow` accepts the effects it declares (`std.net.*`, `std.clock.read`,
`std.random.read`):

```sh
ms add https://github.com/manuscript-lang/ms-pg.git --sha <40-character commit> --allow
```

This writes the following to `ms.toml`; import with `import Config, open from "@pg/conn.ms"`.

```toml
[dependencies]
"@pg" = { url = "https://github.com/manuscript-lang/ms-pg.git", sha = "<40-character commit>" }

[effects]
"@pg" = ["std.net.*", "std.clock.read", "std.random.read"]
```

Import with `import Config, open from "@pg/conn.ms"`. `tests/consumer/run.sh` does this against a local git repository and runs a query.
