# ms-pg

A PostgreSQL client written in Manuscript over the standard library's TCP and TLS. No C and no Rust crates.

Supported: protocol v3 startup, SCRAM-SHA-256 login (the server must prove it knows the password), the simple and
extended query protocols (parse, bind and execute in one round trip), prepared statements, typed parameters, results in text or binary format, transactions, server errors that carry their SQLSTATE in `Error.kind`, and a pool of worker tasks.

## Binary results

Prepared statements (`Conn.prepare` then `Conn.run`, or `Conn.query_cached`) ask for each result column in binary
format when the client has a decoder for its type: `int2`, `int4`, `int8`, `oid`, `float4`, `float8`, `bool`, `bytea`,
`text`, `varchar`, `uuid`, `date`, `timestamp`, `timestamptz`, `numeric`, `interval`, `json` and `jsonb`. Every other
type stays text, chosen per column in the Bind message. Parameters of a prepared statement go out in binary for the
types with an encoder (the statement's parameter types come from the server) and in text otherwise, and a value that
does not fit (an `Int` over `int2` range) is sent as text so the server reports it. `Conn.query` (one round trip,
columns unknown until it returns) stays text. Set `Config.binary_results` to false for text everywhere.

## Typed values

The format changes speed, never the type: `Conn.query`, `Conn.simple`, `Conn.run` and the pool all return the same
`Value` variant for a column, in text or binary. A `timestamp` or `timestamptz` is `Value.Timestamp` (UTC; a `timestamp`
without a zone is read as UTC), a `date` is `Value.Date` (days since 1970-01-01), a `uuid` is `Value.Text`, and
`infinity` values are `Value.Text("infinity")` and `Value.Text("-infinity")`. A `numeric` is `Value.Decimal` (`NaN` and the infinities stay `Text`; a decimal drops trailing zeros), `json` and `jsonb` are `Value.Json(text)`, an `interval` is `Value.Interval(months, days, micros)`, and an array of a known element type is `Value.Array(items)`, nested for more dimensions (arrays read from the text format). A `float4` widens exactly to a `Float`, so
`0.1::float4` is 0.10000000149011612 in both formats. Read values with `value.int()`, `float()`, `bool()`, `text()`,
`bytes()`, `timestamp()`, `date()`, `json()`, `decimal()`, `array()` (each `none` for another variant) and `is_null()`.

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
    print(f"{rows.rows[0][0]}")
    conn.close()?
    return ok()
```

`Pool` (`@pg/pool.ms`) runs statements on worker tasks that each own one connection. A connection cannot move
between tasks, so statements travel to the workers over pipes and rows travel back; one task owns the `Pool`. `query_all` queues at most 32 statements per worker before reading replies, so any number of statements is safe. Pool statements use the text format. `Pool.transaction(statements)` runs them in one transaction on one connection, `Pool.batch(statements)` pipelines them in one round trip, and `Pool.ping()` checks a connection. `start_with(config, size, PoolOptions(max_lifetime: ..., max_idle: ...))` retires old or idle connections, and a worker replaces a connection that failed with a connection error before its next statement. There is no `Acquire`: a connection cannot leave its task, so `transaction` and `batch` send the whole unit of work to it.

## Compatibility with pgx

The names and behavior follow pgx where Manuscript's rules allow.

- **Connection strings**: `parse_config` (`@pg/dsn.ms`) reads `postgres://user:pass@host:port/db?sslmode=verify-full` and `key=value` forms, with several hosts (`host=a,b port=1,2` or `postgres://a:1,b:2/db`) and `target_session_attrs`; `config_from_env(environment()?)` reads the `PG*` variables. Unknown keys become server settings in `Config.params`. `sslmode` is `disable` or `verify-full`; `prefer`, `allow` and `require` are refused, since each falls back to plaintext or skips certificate checks. Give a port list after the host list.
- **Failover**: `open` tries `host`, then `Config.fallbacks`, and takes the first server that matches `target_session_attrs` (`any`, `read-write`, `read-only`, `primary`, `standby`, `prefer-standby`).
- **Client certificates**: `TlsOptions(certificate_pem: ..., private_key_pem: ...)` in `Config.tls`.
- **Unix sockets**: a `host` that starts with `/` names the socket directory.
- **Transactions**: `transaction_with(TxOptions(isolation: Isolation.Serializable, read_only: true), body)`, and `nested(name, body)` for a savepoint that rolls back alone.
- **LISTEN/NOTIFY**: `listen`, `unlisten`, `notify` and `wait_for_notification`. Notifications that arrive during a statement are queued.
- **Cancel**: `cancel(config, pid, secret)` from another task stops a running statement with SQLSTATE `57014`.
- **Batch**: `Batch.add(sql, params)` then `conn.send_batch(batch)` sends every statement and one Sync in one round trip.
- **COPY**: `conn.copy_from(["schema", "table"], columns, rows_stream)` takes a stream, so a pipeline feeds it; `conn.copy_to("copy (select ...) to stdout")` returns the lines. Both use the text format.
- **Statement cache**: `conn.query_cached(sql, params)` prepares each text once (`Config.statement_cache`, default 512), and `conn.deallocate(statement)` frees one.
- **Tracing**: with `Config.trace`, `conn.take_trace()` returns a `TraceEvent` (kind, SQL, elapsed, failure kind) for each statement. Events are plain data rather than a callback, so they work across tasks.
- **Large objects**: `@pg/large.ms` has `lo_create`, `lo_open`, `lo_read`, `lo_write`, `lo_seek`, `lo_tell`, `lo_truncate`, `lo_close` and `lo_unlink`; call them inside a transaction.
- **Rows**: `rows.one()` (like `QueryRow`, with `ENOROWS` and `ETOOMANY`), `rows.column(name)`, `rows.maps()`.
- **Connection**: `conn.ping()`, `conn.parameter(name)`, `conn.in_transaction()`, `quote_ident`.

Not covered: scanning rows into user-defined structs (Manuscript has no reflection, so read `Value`s or use `rows.maps()`), geometric, range, hstore and composite types (they arrive as `Text`), binary COPY, replication, GSSAPI and a password file.

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
