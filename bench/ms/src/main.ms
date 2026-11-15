// Query throughput: WORKERS connections each run QUERIES sequential point lookups with
// one prepared statement.
import (
	promise from "@std/concurrency/promise.ms",
	arguments, environment from "@std/os/env.ms",
	parse_timestamp from "@std/time.ms",

	Config, Conn, open from "@pg/conn.ms",
	Value from "@pg/value.ms",
)

fn config() Config!:
	env := environment()?
	return ok(
		Config(
			host: env.get_or("PGHOST", "127.0.0.1"),
			port: env.get_or("PGPORT", "5432").parse_int().ok_or(
				Error("bad PGPORT")
			)?,
			user: env.get_or("PGUSER", "postgres"),
			password: env.get_or("PGPASSWORD", ""),
			binary_results: env.get_or("BINARY", "1") != "0",
		)
	)

fn worker(id: Int, queries: Int) Int!:
	cfg := config()?
	var conn = open(cfg)?
	statement := conn.prepare(
		"lookup",
		"select id, name, score from items where id = $1",
	)?
	var sum = 0

	for i in 0..queries:
		key := (i * 7919 + id * 104729) % 10000 + 1
		rows := conn.run(statement, [Value.Int(key)])?

		match rows.rows[0][2]:
			Value.Int(score): sum += score
			_: return error(Error("score is not an integer"))

	conn.close()?
	return ok(sum)

// Seconds plus microseconds of a timestamptz column. The text format arrives as `Text`
// and is parsed here, which is what a text-format client pays; the binary format
// arrives decoded.
fn stamp_sum(value: Value) Int!:
	match value:
		Value.Timestamp(stamp): return ok(stamp.seconds + stamp.nanos / 1000)
		Value.Text(text):
			// "2026-01-01 00:00:01.000123+00" is not RFC 3339 yet.
			stamp := parse_timestamp("{text.replace(" ", "T")}:00")?
			return ok(stamp.seconds + stamp.nanos / 1000)
		_: return error(Error("ts is not a timestamp"))

fn int_of(value: Value) Int!:
	match value:
		Value.Int(n): return ok(n)
		_: return error(Error("not an integer"))

fn float_of(value: Value) Float!:
	match value:
		Value.Float(x): return ok(x)
		_: return error(Error("not a float"))

fn scaled(x: Float, factor: Float) Int:
	return (x * factor).truncate_to_int().get(0)

// Reads RANGE consecutive rows of the numbers table per query and sums every column.
fn numbers_worker(id: Int, queries: Int) Int!:
	var conn = open(config()?)?
	void conn.simple("set time zone 'UTC'")?

	statement := conn.prepare(
		"range",
		"select id, a, b, c, ts from readings where id between $1 and $2",
	)?
	var sum = 0

	for i in 0..queries:
		low := (i * 7919 + id * 104729) % 99000 + 1
		rows := conn.run(statement, [Value.Int(low), Value.Int(low + 999)])?

		for row in rows.rows:
			sum += int_of(row[0])? + int_of(row[1])?
			sum += scaled(float_of(row[2])?, 2.0) + scaled(float_of(row[3])?, 4.0)
			sum += stamp_sum(row[4])?

	conn.close()?
	return ok(sum)

// Creates the table the lookups read: 10000 rows of (id, name, score).
fn setup(cfg: Config) Void!:
	var conn = open(cfg)?
	void conn.simple("drop table if exists items")?
	void conn.simple(
		"create table items (id int8 primary key, name text not null, score int8 not null)"
	)?
	void conn.simple(
		"insert into items select g, 'item-' || g, (g * 37) % 1000 from generate_series(1, 10000) g"
	)?
	void conn.simple("analyze items")?
	void conn.simple("drop table if exists readings")?
	void conn.simple(
		"create table readings (id int8 primary key, a int4 not null, b float8 not null, c float4 not null, ts timestamptz not null)"
	)?
	void conn.simple(
		"insert into readings select g, (g * 7919) % 100003 - 50000, g * 0.5 - 25000, ((g % 1000) - 500) * 0.25, timestamptz '2026-01-01 00:00:00+00' + g::int8 * 1000123 * interval '1 microsecond' from generate_series(1, 100000) g"
	)?
	void conn.simple("analyze readings")?
	conn.close()?
	return ok()

fn main() Void!:
	args := environment()?
	workers := args.get_or("WORKERS", "4").parse_int().ok_or(Error("bad WORKERS"))?
	queries := args.get_or("QUERIES", "20000").parse_int().ok_or(Error("bad QUERIES"))?
	cfg := config()?
	args_list := arguments()
	if args_list.length() > 0 and args_list[0] == "setup": return setup(cfg)

	numbers := args.get_or("BENCH", "lookup") == "numbers"
	tasks := (0..workers).map(
		fn id: promise(
			fn: if numbers: numbers_worker(id, queries) else: worker(
				id,
				queries,
			)
		)
	)
	var total = 0
	for task in tasks: total += task.wait()?
	print("checksum {total}")
	return ok()
