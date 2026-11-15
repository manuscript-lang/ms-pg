# Postgres client throughput

This package's client against Go's `pgx` and Rust's `tokio-postgres`: 4 connections, each running 20000 sequential point lookups (`select id, name, score from items where id = $1`, one prepared statement, one round trip per lookup) on a 10000-row table. Each program opens its own connections, so connect and login are in the time. Every program prints the checksum of the scores it read, and the driver fails on a mismatch.

```sh
eval $(tests/start-postgres.sh /tmp/pg)   # starts a server and exports the PG* variables
MS=path/to/ms bench/run.sh 25
```

`run.sh` copies `src` into `bench/ms/pg` (a project keeps its imports inside its root), builds the three programs into `.build/`, creates the table, and runs them in rotating order, reporting the median, minimum and maximum. `WORKERS` and `QUERIES` change the shape.

Designs match: one connection per worker, one prepared statement, strictly sequential lookups. Differences that are not the language: pgx and tokio-postgres fetch results in the binary format, and the Manuscript client now does too for prepared statements (`BINARY=0` switches it to text); the server runs on the same machine and shares its 4 cores with the clients; `tokio-postgres` spawns a task per connection on a multi-thread runtime.

## Point lookups after the scheduler fix, 2026-11-15, same machine, manuscript revision 2283f91f

manuscript #4117 fixed the runtime starting four waiting promises on two or three threads, which caused most of the run-to-run swings below. Median of 25 alternating runs, with the fastest and slowest run:

| Program | Median | Min | Max | Lookups per second (median) |
| --- | --- | --- | --- | --- |
| Manuscript (binary results) | 0.671 s | 0.575 s | 2.064 s | 119,282 |
| Go pgx v5.13.0 | 1.033 s | 0.823 s | 1.422 s | 77,440 |
| Rust tokio-postgres 0.7.18 | 1.674 s | 1.549 s | 2.058 s | 47,803 |

Manuscript is now clearly ahead of pgx on the median here, with its slowest run in 25 (2.06 s) still above pgx's. A separate 25-run comparison of the runtime before and after the fix, same program: median 0.947 s to 0.645 s, slowest 1.595 s to 1.029 s. Treat the tail as unexplained; the machine is shared with the server.

## Point lookups before the scheduler fix, 2026-11-15, Linux amd64, 4 cores (Xeon 2.10 GHz), PostgreSQL 16 on localhost, manuscript revision ff75ca4d

Median of 25 alternating runs, with the fastest and slowest run:

| Program | Median | Min | Max | Lookups per second (median) |
| --- | --- | --- | --- | --- |
| Manuscript (binary results) | 1.311 s | 0.565 s | 1.980 s | 61,031 |
| Go pgx v5.13.0 | 1.055 s | 0.717 s | 1.355 s | 75,805 |
| Rust tokio-postgres 0.7.18 | 1.699 s | 1.519 s | 1.849 s | 47,081 |

Before the scheduler fix, this benchmark did not show a winner between Manuscript and pgx. The shared VM moves Manuscript between 0.57 s and 1.98 s from run to run, wider than any gap between the two, and the lookups are network-bound, so text versus binary makes no reliable difference either (7 alternating runs: median 1.165 s text, 1.036 s binary). An earlier single dated run (0.687 s Manuscript, 0.884 s pgx) fell inside this spread and is not a lead. Only tokio-postgres is consistently slower; the likely cause is its runtime sharing the 4 cores with the server, not the library, and that is not investigated. Instruction counts (`valgrind --tool=callgrind`) have not been taken.

## Numbers: ints, floats and timestamps

`BENCH=numbers bench/run.sh 25` reads `select id, a, b, c, ts from readings where id between $1 and $2`, 1000 rows per statement, from a 100000-row table of `int8`, `int4`, `float8`, `float4` and `timestamptz` columns. 4 connections each run 200 statements (800,000 rows per run), so decoding is most of the work. Every program sums every column and the driver fails on a mismatch; the floats are exact in binary, so text and binary agree. In the Manuscript text row the client parses the timestamp text into `Value.Timestamp`, so both rows return the same typed values; the table stores timestamps with microseconds. tokio-postgres reads the timestamp as raw microseconds (a custom `FromSql`) because its date types need an extra crate.

### 2026-11-15, same machine and revision

Median of 25 alternating runs, with the fastest and slowest run:

| Program | Median | Min | Max | Rows per second (median) |
| --- | --- | --- | --- | --- |
| Manuscript, binary | 0.346 s | 0.263 s | 0.515 s | 2,313,244 |
| Manuscript, text | 0.656 s | 0.469 s | 0.845 s | 1,219,026 |
| Go pgx v5.13.0 | 0.306 s | 0.267 s | 0.376 s | 2,618,527 |
| Rust tokio-postgres 0.7.18 | 0.239 s | 0.167 s | 0.291 s | 3,340,626 |

Binary is about 1.9x faster than text for Manuscript here, and the ranges barely overlap (binary at most 0.515 s, text at least 0.469 s). Against pgx the medians are 13% apart but the ranges overlap, so call it close; tokio-postgres is ahead of both. Run-to-run spread on this shared VM is large, so read ratios between rows, not absolute times.

### After the scheduler fix (manuscript #4117), 25 runs

| Program | Median | Min | Max | Rows per second (median) |
| --- | --- | --- | --- | --- |
| Manuscript, binary | 0.338 s | 0.278 s | 0.477 s | 2,366,333 |
| Manuscript, text | 0.560 s | 0.396 s | 0.772 s | 1,428,079 |
| Go pgx v5.13.0 | 0.315 s | 0.281 s | 0.417 s | 2,537,016 |
| Rust tokio-postgres 0.7.18 | 0.248 s | 0.204 s | 0.277 s | 3,225,283 |

Binary is 1.7x faster than text, about 7% behind pgx (ranges overlap), and behind tokio-postgres. The scheduler fix changed little here: each worker has a long result to decode, so the numbers path was already close to four threads.

### 2026-11-15, typed text results, manuscript revision 37fcf69b

The Manuscript client now decodes text-format timestamps itself (so the text row no longer parses them in the benchmark program) and the benchmark no longer sets the session time zone. Median of 15 alternating runs on a 4-core shared VM:

| Program | Median | Min | Max | Rows per second (median) |
| --- | --- | --- | --- | --- |
| Manuscript, binary | 0.328 s | 0.230 s | 0.367 s | 2,438,003 |
| Manuscript, text | 0.605 s | 0.458 s | 0.972 s | 1,321,761 |
| Go pgx v5.13.0 | 0.297 s | 0.262 s | 0.349 s | 2,695,358 |
| Rust tokio-postgres 0.7.18 | 0.215 s | 0.188 s | 0.275 s | 3,721,178 |

Point lookups on the same machine, 15 runs: Manuscript 0.621 s, pgx 0.935 s, tokio-postgres 1.543 s.
