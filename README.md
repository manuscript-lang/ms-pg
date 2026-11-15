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

A binary `timestamp` or `timestamptz` arrives as `Value.Timestamp` (UTC) and a `date` as `Value.Date` (days since
1970-01-01); in text format both are `Value.Text`, because their meaning depends on the session time zone. A `uuid` is
`Value.Text` in both formats, and `infinity` values are `Value.Text("infinity")` and `Value.Text("-infinity")`.

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
between tasks, so statements travel to the workers over pipes and rows travel back; one task owns the `Pool`.

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
