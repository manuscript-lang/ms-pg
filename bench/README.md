# Postgres client throughput

This package's client against Go's `pgx` and Rust's `tokio-postgres`: 4 connections, each running 20000 sequential point lookups (`select id, name, score from items where id = $1`, one prepared statement, one round trip per lookup) on a 10000-row table. Each program opens its own connections, so connect and login are in the time. Every program prints the checksum of the scores it read, and the driver fails on a mismatch.

```sh
eval $(tests/start-postgres.sh /tmp/pg)   # starts a server and exports the PG* variables
MS=path/to/ms bench/run.sh 9
```

`run.sh` copies `src` into `bench/ms/pg` (a project keeps its imports inside its root), builds the three programs into `.build/`, creates the table, and runs them in rotating order, reporting medians. `WORKERS` and `QUERIES` change the shape.

Designs match: one connection per worker, one prepared statement, strictly sequential lookups. Differences that are not the language: pgx and tokio-postgres fetch results in the binary format, and the Manuscript client now does too for prepared statements (`BINARY=0` switches it to text); the server runs on the same machine and shares its 4 cores with the clients; `tokio-postgres` spawns a task per connection on a multi-thread runtime.

## 2026-11-15, Linux amd64, 4 cores (Xeon 2.10 GHz), PostgreSQL 16 on localhost, manuscript revision 3dde763e

| Program | Median of 9 | Lookups per second |
| --- | --- | --- |
| Manuscript | 0.687 s | 116,393 |
| Go pgx v5.13.0 | 0.884 s | 90,486 |
| Rust tokio-postgres 0.7.18 | 1.523 s | 52,520 |

A first run of 5 samples gave 1.082 s, 0.860 s and 1.424 s: Manuscript moved by 1.6x between runs on this shared VM, so treat the order of Manuscript and pgx as unsettled. The slow tokio-postgres result held in both runs and has not been investigated; the likely cause is the runtime sharing the 4 cores with the server, not the library. Instruction counts (`valgrind --tool=callgrind`) have not been taken.

## Numbers: ints, floats and timestamps

`BENCH=numbers bench/run.sh 9` reads `select id, a, b, c, ts from readings where id between $1 and $2`, 1000 rows per statement, from a 100000-row table of `int8`, `int4`, `float8`, `float4` and `timestamptz` columns. 4 connections each run 200 statements (800,000 rows per run), so decoding is most of the work. Every program sums every column and the driver fails on a mismatch; the floats are exact in binary, so text and binary agree. The Manuscript text row parses the timestamp text itself, which is what a text-format client pays; the table stores timestamps with microseconds. tokio-postgres reads the timestamp as raw microseconds (a custom `FromSql`) because its date types need an extra crate.

### 2026-11-15, Linux amd64, 4 cores (Xeon 2.10 GHz), PostgreSQL 16 on localhost, manuscript revision ff75ca4d

| Program | Median of 9 | Rows per second |
| --- | --- | --- |
| Manuscript, binary | 0.344 s | 2,328,153 |
| Manuscript, text | 0.583 s | 1,371,448 |
| Go pgx v5.13.0 | 0.253 s | 3,162,595 |
| Rust tokio-postgres 0.7.18 | 0.187 s | 4,273,056 |

Binary is 1.7x faster than text for Manuscript on this workload, and still 1.3x behind pgx and 1.8x behind tokio-postgres. Same shared-VM caveat as above: a 3-sample run gave 0.374 s binary and 0.433 s text, so the size of the gap moves between runs; its direction did not. The point-lookup benchmark above is network-bound and shows no reliable difference between text and binary here (7 alternating runs: median 1.165 s text, 1.036 s binary, spread of 0.6 to 1.3 s).
