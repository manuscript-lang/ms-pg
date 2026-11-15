import (
	Bytes from "@std/bytes.ms",
	Promise, promise from "@std/concurrency/promise.ms",
	collect, filter, fold, from_list, map, take from "@std/io/streams.ms",
	parse_decimal from "@std/math/decimal.ms",
	Connection, listen from "@std/net/net.ms",
	TlsOptions from "@std/net/tls.ms",
	environment from "@std/os/env.ms",
	read_file from "@std/os/fs.ms",
	expect_equal, expect_error, expect_true from "@std/test/test.ms",
	Timestamp, sleep from "@std/time.ms",

	md5_password from "@pg/auth.ms",
	Batch,
	Config,
	Conn,
	Endpoint,
	Isolation,
	TxOptions,
	cancel,
	open from "@pg/conn.ms",
	config_from_env, parse_config from "@pg/dsn.ms",
	interpolate, named from "@pg/sql.ms",
	LoMode,
	Whence,
	lo_close,
	lo_create,
	lo_open,
	lo_read,
	lo_seek,
	lo_tell,
	lo_truncate,
	lo_unlink,
	lo_write from "@pg/large.ms",
	Pool, PoolOptions, Statement, start, start_with from "@pg/pool.ms",
	stream from "@pg/stream.ms",
	Value from "@pg/value.ms",
	Backend, DATA_ROW, decode_message from "@pg/wire.ms",
)

// Connection settings come from PGHOST, PGPORT, PGUSER, PGPASSWORD and PGDATABASE.
fn config() Config!:
	env := environment()?
	return ok(
		Config(
			host: env.get_or("PGHOST", "127.0.0.1"),
			port: env.get_or("PGPORT", "5432").parse_int().get(5432),
			user: env.get_or("PGUSER", "postgres"),
			password: env.get_or("PGPASSWORD", ""),
			database: env.get_or("PGDATABASE", "postgres"),
		)
	)

test "md5 password response matches the server algorithm":
	salt := Bytes([1, 2, 3, 4])
	expect_equal(
		md5_password("postgres", "secret", salt),
		"md5bb41a296aab6baccb36ff243a562abff",
	)?

test "a recorded DataRow decodes with a NULL column":
	// two columns: "ab" and NULL
	body := Bytes([0, 2, 0, 0, 0, 2, 97, 98, 255, 255, 255, 255])

	match decode_message(DATA_ROW, body)?:
		Backend.DataRow(columns):
			expect_equal(columns.length(), 2)?
			expect_true(columns[1].is_none())?
		_: expect_true(false)?

test "extended query returns typed values":
	var conn = open(config()?)?
	rows := conn.query(
		"select $1::int8 + 1, $2::text, $3::bool, $4::float8, null::int, $5::bytea",
		[
			Value.Int(41),
			Value.Text("hi"),
			Value.Bool(true),
			Value.Float(1.5),
			Value.Bytes(Bytes([0, 255])),
		],
	)?
	expect_equal(rows.rows[0][0], Value.Int(42))?
	expect_equal(rows.rows[0][1], Value.Text("hi"))?
	expect_equal(rows.rows[0][2], Value.Bool(true))?
	expect_equal(rows.rows[0][3], Value.Float(1.5))?
	expect_equal(rows.rows[0][4], Value.Null)?
	expect_equal(rows.rows[0][5], Value.Bytes(Bytes([0, 255])))?
	expect_equal(rows.command, "SELECT 1")?
	conn.close()?

test "a server error carries its SQLSTATE and leaves the connection usable":
	var conn = open(config()?)?
	failure := expect_error(conn.query("select 1/0", []))?
	expect_equal(failure.kind, "22012")?

	rows := conn.query("select 7", [])?
	expect_equal(rows.rows[0][0], Value.Int(7))?
	conn.close()?

test "a prepared statement runs repeatedly with typed results":
	var conn = open(config()?)?
	statement := conn.prepare("double", "select $1::int * 2 as twice")?

	for n in 0..3:
		rows := conn.run(statement, [Value.Int(n)])?
		expect_equal(rows.rows[0][0], Value.Int(n * 2))?
		expect_equal(rows.columns[0], "twice")?

	conn.close()?

test "transactions commit and roll back":
	var conn = open(config()?)?
	void conn.simple("create temp table t (n int)")?
	conn.begin()?
	void conn.query("insert into t values ($1)", [Value.Int(1)])?
	conn.rollback()?

	none_left := conn.query("select count(*) from t", [])?
	expect_equal(none_left.rows[0][0], Value.Int(0))?
	conn.begin()?
	void conn.query("insert into t values ($1)", [Value.Int(1)])?
	conn.commit()?

	one_left := conn.query("select count(*) from t", [])?
	expect_equal(one_left.rows[0][0], Value.Int(1))?
	conn.close()?

test "a wrong password is rejected":
	cfg := config()?
	failure := expect_error(
		open(
			Config(
				host: cfg.host.clone(),
				port: cfg.port,
				user: cfg.user.clone(),
				password: "wrong",
			)
		)
	)?
	expect_equal(failure.kind, "28P01")?

test "a pool runs statements concurrently and returns results in order":
	var pool = start(config()?, 3)?
	statements := (0..7).map(
		fn n: Statement("select $1::int, pg_sleep(0.05)", [Value.Int(n)])
	)
	results := pool.query_all(statements)?
	expect_equal(results.length(), 7)?
	for n in 0..7: expect_equal(results[n].rows[0][0], Value.Int(n))?

	single := pool.query("select 5::int")?
	expect_equal(single.rows[0][0], Value.Int(5))?
	pool.close()?

test "a pool reports a server error and keeps working":
	var pool = start(config()?, 2)?
	failure := expect_error(pool.query("select 1/0"))?
	expect_equal(failure.kind, "22012")?

	ok_rows := pool.query("select 1")?
	expect_equal(ok_rows.rows[0][0], Value.Int(1))?
	pool.close()?

fn login(user: String, password: String, allow_md5: Bool) Conn!:
	cfg := config()?
	return open(
		Config(
			host: cfg.host.clone(),
			port: cfg.port,
			user: user,
			password: password,
			allow_md5: allow_md5,
		)
	)

test "md5 login is refused unless allowed":
	failure := expect_error(login("md5user", "md5pass", false))?
	expect_equal(failure.kind, "EACCES")?

	var conn = login("md5user", "md5pass", true)?
	rows := conn.query("select current_user", [])?
	expect_equal(rows.rows[0][0], Value.Text("md5user"))?
	conn.close()?

test "a cleartext password is refused without TLS":
	failure := expect_error(login("clearuser", "clearpass", true))?
	expect_equal(failure.kind, "EACCES")?

fn impostor_accept(accepted: Promise[Connection]) Void!:
	var client = accepted.wait()?
	return impostor(var client)

// A server that offers SCRAM and then says "authenticated" without ever proving it
// knows the password.
fn impostor(var client: Connection) Void!:
	void client.input.next()?

	// R, length 23, method 10, "SCRAM-SHA-256", two NULs
	offer := Bytes(
		[
			82, 0, 0, 0, 23, 0, 0, 0, 10, 83, 67, 82, 65, 77, 45, 83, 72, 65, 45, 50,
			53, 54, 0, 0
		]
	)
	client.output.write(offer)?
	void client.input.next()?
	client.output.write(Bytes([82, 0, 0, 0, 8, 0, 0, 0, 0]))?
	client.close()?
	return ok()

test "a server that skips the SCRAM proof is rejected":
	var listener = listen("127.0.0.1:0")?
	accepted := listener.accept()
	port := listener.address().split(":")[1].parse_int().ok_or(Error("bad address"))?
	accepting := promise(fn: impostor_accept(accepted))
	failure := expect_error(open(Config(port: port, password: "secret")))?
	expect_equal(failure.kind, "EACCES")?
	accepting.wait()?
	listener.close()?

test "transaction commits on success and rolls back on error":
	var conn = open(config()?)?
	void conn.simple("create temp table tx (n int)")?

	added := conn.transaction(
		fn(var c: Conn): c.query("insert into tx values (1)", [])
	)?
	expect_equal(added.command, "INSERT 0 1")?
	void expect_error(
		conn.transaction(
			fn(var c: Conn): c.query("insert into tx values (1/0)", [])
		)
	)?

	count := conn.query("select count(*) from tx", [])?
	expect_equal(count.rows[0][0], Value.Int(1))?
	conn.close()?

test "TLS login upgrades the socket after an SSLRequest":
	env := environment()?
	roots := (read_file(env.get_or("PGSSLROOTCERT", "")).wait()?).decode_utf8().ok_or(
		Error("bad certificate")
	)?
	cfg := config()?
	var conn = open(
		Config(
			host: cfg.host.clone(),
			port: cfg.port,
			user: cfg.user.clone(),
			password: cfg.password.clone(),
			tls: TlsOptions(server_name: "localhost", roots_pem: roots),
		)
	)?
	rows := conn.query("select ssl from pg_stat_ssl where pid = pg_backend_pid()", [])?
	expect_equal(rows.rows[0][0], Value.Bool(true))?
	conn.close()?

// A server that answers the SSL request with `N`: no TLS here.
fn refuser(var client: Connection) Void!:
	void client.input.next()?
	client.output.write(Bytes([78]))?
	client.close()?
	return ok()

fn refuser_accept(accepted: Promise[Connection]) Void!:
	var client = accepted.wait()?
	return refuser(var client)

test "a server that refuses TLS is an error and not a plaintext fallback":
	var listener = listen("127.0.0.1:0")?
	accepted := listener.accept()
	port := listener.address().split(":")[1].parse_int().ok_or(Error("bad address"))?
	accepting := promise(fn: refuser_accept(accepted))
	failure := expect_error(
		open(Config(port: port, tls: TlsOptions(server_name: "localhost")))
	)?
	expect_equal(failure.kind, "ENOTSUP")?
	accepting.wait()?
	listener.close()?

fn first_row(var conn: Conn, sql: String) List[Value]!:
	statement := conn.prepare("", sql)?
	rows := conn.run(statement, [])?
	return ok(rows.rows[0].clone())

test "binary results decode the common types":
	var conn = open(config()?)?
	row := first_row(
		var conn,
		"select 1::int2, -2::int4, 9007199254740993::int8, 1.5::float4, -2.25::float8, true, '\\x00ff'::bytea, 'héllo'::text, 'v'::varchar, '123e4567-e89b-12d3-a456-426614174000'::uuid, 12::oid, null::int8",
	)?
	expect_equal(row[0], Value.Int(1))?
	expect_equal(row[1], Value.Int(-2))?
	expect_equal(row[2], Value.Int(9007199254740993))?
	expect_equal(row[3], Value.Float(1.5))?
	expect_equal(row[4], Value.Float(-2.25))?
	expect_equal(row[5], Value.Bool(true))?
	expect_equal(row[6], Value.Bytes(Bytes([0, 255])))?
	expect_equal(row[7], Value.Text("héllo"))?
	expect_equal(row[8], Value.Text("v"))?
	expect_equal(row[9], Value.Text("123e4567-e89b-12d3-a456-426614174000"))?
	expect_equal(row[10], Value.Int(12))?
	expect_equal(row[11], Value.Null)?
	conn.close()?

test "binary results decode timestamps and dates":
	var conn = open(config()?)?
	row := first_row(
		var conn,
		"select '2026-11-15 12:34:56.789012+00'::timestamptz, '1969-12-31 23:59:59.5'::timestamp, '2000-01-01'::date, '1970-01-02'::date, 'infinity'::timestamptz",
	)?
	expect_equal(
		row[0],
		Value.Timestamp(Timestamp(seconds: 1794746096, nanos: 789012000)),
	)?
	expect_equal(row[1], Value.Timestamp(Timestamp(seconds: -1, nanos: 500000000)))?
	expect_equal(row[2], Value.Date(10957))?
	expect_equal(row[3], Value.Date(1))?
	expect_equal(row[4], Value.Text("infinity"))?
	conn.close()?

test "binary and text results agree":
	var binary = open(config()?)?
	cfg := config()?
	var text = open(
		Config(
			host: cfg.host.clone(),
			port: cfg.port,
			user: cfg.user.clone(),
			password: cfg.password.clone(),
			binary_results: false,
		)
	)?
	sql := "select 7::int4, 0.1::float8, 'x'::text, 1.25::numeric, 'e'::bytea, 0.1::float4, 16777217::float4, '2026-11-15 12:34:56.789012+05:30'::timestamptz, '1969-12-31 23:59:59.5'::timestamp, '2000-03-01'::date, 'infinity'::timestamptz, '123e4567-e89b-12d3-a456-426614174000'::uuid"
	expect_equal(first_row(var binary, sql)?, first_row(var text, sql)?)?

	row := first_row(var binary, sql)?
	expect_equal(row[3].decimal().get(parse_decimal("0")?).to_string(), "1.25")?

	// NaN never equals itself, so compare it by `is_nan` in both formats.
	nan := "select 'NaN'::float8, 'Infinity'::float8"

	for pair in [first_row(var binary, nan)?, first_row(var text, nan)?]:
		expect_equal(pair[0].float().get(0.0).is_nan(), true)?
		expect_equal(pair[1].float().get(0.0).is_finite(), false)?

	binary.close()?
	text.close()?

test "every call path returns the same typed values":
	var conn = open(config()?)?
	sql := "select 0.1::float4, '2026-11-15 12:34:56.789012+05:30'::timestamptz, '2000-03-01'::date, 12::int8, null::text"
	typed := first_row(var conn, sql)?

	// The one-round-trip path and the simple protocol return text-format columns.
	queried := conn.query(sql, [])?
	expect_equal(queried.rows[0], typed)?

	simple := conn.simple(sql)?
	expect_equal(simple.rows[0], typed)?

	// A float4 widens exactly, so 0.1::float4 is not the double 0.1, in either format.
	expect_equal(typed[0].float(), 0.10000000149011612)?
	expect_equal(typed[1].timestamp().is_some(), true)?
	expect_equal(typed[2].date(), 11017)?
	expect_equal(typed[3].int(), 12)?
	expect_equal(typed[4].is_null(), true)?
	expect_equal(typed[3].text(), none)?
	conn.close()?

test "a pool with no workers is an error":
	failure := expect_error(start(config()?, 0))?
	expect_equal(failure.kind, "EINVAL")?

test "a pool runs more statements than its pipes hold":
	var pool = start(config()?, 2)?
	statements := (0..400).map(fn n: Statement("select $1::int", [Value.Int(n)]))
	results := pool.query_all(statements)?
	expect_equal(results.length(), 400)?
	for n in 0..400: expect_equal(results[n].rows[0][0], Value.Int(n))?
	pool.close()?

test "rows compose with stream pipelines":
	var conn = open(config()?)?
	rows := conn.query("select n, n * 2 from generate_series(1, 10) n", [])?
	evens := collect(
		rows.stream() | filter(fn row: row[0].int().get(0) % 2 == 0) | map(
			fn row: (row[1].int().get(0))
		),
		max_items: 10,
	)?
	expect_equal(evens, [4, 8, 12, 16, 20])?

	total := fold(
		rows.stream() | map(fn row: row[1].int().get(0)),
		0,
		fn sum,
		n: (sum + n)
	)?
	expect_equal(total, 110)?
	conn.close()?

test "stream sends rows as the server produces them":
	// 100000 rows through a 16-row pipe: the stream never holds them all.
	total := fold(
		stream(
			config()?,
			"select n from generate_series(1, 100000) n",
			[],
			16,
		) | map(fn row: (row[0].int().get(0))),
		0,
		fn sum,
		n: sum + n
	)?
	expect_equal(total, 5000050000)?

	first := collect(
		stream(
			config()?,
			"select n from generate_series(1, 100000) n",
			[],
			16,
		) | take(3),
		max_items: 10,
	)?
	expect_equal(first.length(), 3)?

test "a stream ends with the server error":
	failure := expect_error(collect(stream(config()?, "select 1/0", []), max_items: 10))?
	expect_equal(failure.kind, "22012")?

test "a URL connection string sets every part":
	parsed := parse_config(
		"postgres://app:p%40ss@db.example:6543/shop?application_name=web&statement_timeout=5s&sslmode=verify-full"
	)?
	expect_equal(parsed.user, "app")?
	expect_equal(parsed.password, "p@ss")?
	expect_equal(parsed.host, "db.example")?
	expect_equal(parsed.port, 6543)?
	expect_equal(parsed.database, "shop")?
	expect_equal(parsed.application_name, "web")?
	expect_equal(parsed.params.get_or("statement_timeout", ""), "5s")?
	expect_true(parsed.tls.is_some())?

test "a URL with an IPv6 host and no login":
	parsed := parse_config("postgresql://[::1]:5433/db")?
	expect_equal(parsed.host, "::1")?
	expect_equal(parsed.port, 5433)?
	expect_equal(parsed.user, "postgres")?
	expect_true(parsed.tls.is_none())?

test "a key value connection string with quotes":
	parsed := parse_config(
		"host=localhost port=5433 user=app password='a b' dbname=shop sslmode=disable"
	)?
	expect_equal(parsed.host, "localhost")?
	expect_equal(parsed.password, "a b")?
	expect_equal(parsed.database, "shop")?
	expect_true(parsed.tls.is_none())?

test "sslmodes that would weaken TLS are refused":
	void expect_error(parse_config("host=h sslmode=prefer"))?
	void expect_error(parse_config("host=h sslmode=require"))?
	void expect_error(parse_config("host=h port=abc"))?

test "libpq environment variables fill the config":
	var env = Map[String, String]()
	env.add("PGHOST", "db")
	env.add("PGPORT", "6000")
	env.add("PGSSLMODE", "verify-full")

	parsed := config_from_env(env)?
	expect_equal(parsed.host, "db")?
	expect_equal(parsed.port, 6000)?
	expect_true(parsed.tls.is_some())?

test "a read only transaction rejects writes":
	var conn = open(config()?)?
	void conn.simple("create temp table ro (n int)")?

	failure := expect_error(
		conn.transaction_with(
			TxOptions(isolation: Isolation.Serializable, read_only: true),
			fn(var c: Conn): c.query("create schema ro_probe", [])
		)
	)?
	expect_equal(failure.kind, "25006")?

	level := conn.transaction_with(
		TxOptions(isolation: Isolation.RepeatableRead),
		fn(var c: Conn): c.query("show transaction_isolation", [])
	)?
	expect_equal(level.rows[0][0].text().get(""), "repeatable read")?
	conn.close()?

test "a nested transaction rolls back to its savepoint":
	var conn = open(config()?)?
	void conn.simple("create temp table sp (n int)")?

	void conn.transaction(
		fn(var c: Conn):
			void c.query("insert into sp values (1)", [])?
			void c.nested(
				"inner",
				fn(var d: Conn): d.query(
					"insert into sp values (2)",
					[],
				)
			)?
			void c.nested(
				"failing",
				fn(var d: Conn): d.query(
					"insert into sp values (1/0)",
					[],
				)
			)
			return c.query("insert into sp values (3)", [])
	)?

	rows := conn.query("select n from sp order by n", [])?
	expect_equal(rows.rows.length(), 3)?
	expect_equal(rows.rows[2][0].int().get(0), 3)?
	conn.close()?

test "listen delivers notifications from another connection":
	var listener = open(config()?)?
	var sender = open(config()?)?
	listener.listen("jobs")?
	sender.notify("jobs", "hello")?

	got := listener.wait_for_notification()?
	expect_equal(got.channel, "jobs")?
	expect_equal(got.payload, "hello")?
	expect_equal(got.pid, sender.pid)?

	// A notification that arrives during a query is queued, not lost.
	sender.notify("jobs", "queued")?
	void listener.query("select 1", [])?

	again := listener.wait_for_notification()?
	expect_equal(again.payload, "queued")?
	listener.close()?
	sender.close()?

fn cancel_soon(config: Config, pid: Int, secret: Int) Void!:
	sleep(300.milliseconds()).wait()?
	return cancel(config, pid, secret)

test "cancel stops a running statement":
	settings := config()?
	var conn = open(settings)?
	pid := conn.pid
	secret := conn.secret
	target := settings.clone()
	stopping := promise(fn: cancel_soon(target, pid, secret))
	failure := expect_error(conn.query("select pg_sleep(30)", []))?
	expect_equal(failure.kind, "57014")?
	stopping.wait()?

	back := conn.query("select 1", [])?
	expect_equal(back.rows[0][0].int().get(0), 1)?
	conn.close()?

test "a batch runs every statement in one round trip":
	var conn = open(config()?)?
	void conn.simple("create temp table bt (n int)")?

	var batch = Batch(sql: [], params: [])
	batch.add("insert into bt values ($1)", [Value.Int(1)])
	batch.add("insert into bt values ($1)", [Value.Int(2)])
	batch.add("select sum(n) from bt")

	results := conn.send_batch(batch)?
	expect_equal(results.length(), 3)?
	expect_equal(results[0].command, "INSERT 0 1")?
	expect_equal(results[2].rows[0][0].int().get(0), 3)?

	var bad = Batch(sql: [], params: [])
	bad.add("insert into bt values (3)")
	bad.add("select 1/0")
	void expect_error(conn.send_batch(bad))?

	count := conn.query("select count(*) from bt", [])?
	expect_equal(count.rows[0][0].int().get(0), 2)?
	conn.close()?

test "copy from loads rows from a stream and copy to reads them back":
	var conn = open(config()?)?
	void conn.simple("create temp table cp (id int, note text)")?

	rows := [
		[Value.Int(1), Value.Text("a\tb")],
		[Value.Int(2), Value.Null],
		[Value.Int(3), Value.Text("line\nbreak \\ slash")],
	]
	stored := conn.copy_from(["cp"], ["id", "note"], from_list(rows))?
	expect_equal(stored, 3)?

	back := conn.copy_to("copy (select id, note from cp order by id) to stdout")?
	expect_equal(back.rows.length(), 3)?
	expect_equal(back.rows[0][1].text().get(""), "a\tb")?
	expect_true(back.rows[1][1].is_null())?
	expect_equal(back.rows[2][1].text().get(""), "line\nbreak \\ slash")?

	failure := expect_error(
		conn.copy_from(["cp"], ["id"], from_list([[Value.Text("x")]]))
	)?
	expect_equal(failure.kind, "22P02")?

	count := conn.query("select count(*) from cp", [])?
	expect_equal(count.rows[0][0].int().get(0), 3)?
	conn.close()?

test "numeric, json, interval and arrays decode the same in both formats":
	var binary = open(config()?)?
	cfg := config()?
	var text = open(
		Config(
			host: cfg.host.clone(),
			port: cfg.port,
			user: cfg.user.clone(),
			password: cfg.password.clone(),
			binary_results: false,
		)
	)?
	sql := "select 12345678901234567890.0012::numeric, (-0.000123)::numeric, 0::numeric, 100000::numeric, '{\"a\": [1, 2]}'::jsonb, '{\"b\": 1}'::json, '1 year 2 mons 3 days 04:05:06.7'::interval, '-1 days -00:00:01'::interval, '{1,2,NULL}'::int4[], '{{\"a b\",\"c\\\"d\"},{e,NULL}}'::text[], '{}'::int8[], 'NaN'::numeric"
	typed := first_row(var binary, sql)?
	expect_equal(typed, first_row(var text, sql)?)?
	expect_equal(
		typed[0].decimal().get(parse_decimal("0")?).to_string(),
		"12345678901234567890.0012",
	)?
	expect_equal(typed[1].decimal().get(parse_decimal("0")?).to_string(), "-0.000123")?
	expect_equal(typed[3].decimal().get(parse_decimal("0")?).to_string(), "100000")?
	expect_equal(typed[4].json().get(""), "{\"a\": [1, 2]}")?
	expect_equal(typed[6], Value.Interval(14, 3, 14706700000))?
	expect_equal(typed[7], Value.Interval(0, -1, -1000000))?
	expect_equal(typed[8], Value.Array([Value.Int(1), Value.Int(2), Value.Null]))?

	nested := typed[9].array().get([])
	expect_equal(nested[0], Value.Array([Value.Text("a b"), Value.Text("c\"d")]))?
	expect_equal(nested[1], Value.Array([Value.Text("e"), Value.Null]))?
	expect_equal(typed[10], Value.Array([]))?
	expect_equal(typed[11], Value.Text("NaN"))?
	binary.close()?
	text.close()?

test "arrays, json, decimals and intervals go out as parameters":
	var conn = open(config()?)?
	rows := conn.query(
		"select $1::int4[], $2::jsonb, $3::numeric, $4::interval",
		[
			Value.Array([Value.Int(1), Value.Null, Value.Int(3)]),
			Value.Json("{\"k\": true}"),
			Value.Decimal(parse_decimal("1.50")?),
			Value.Interval(1, 2, 3),
		],
	)?
	expect_equal(rows.rows[0][0], Value.Array([Value.Int(1), Value.Null, Value.Int(3)]))?
	expect_equal(rows.rows[0][1].json().get(""), "{\"k\": true}")?
	expect_equal(rows.rows[0][2].decimal().get(parse_decimal("0")?).to_string(), "1.5")?
	expect_equal(rows.rows[0][3], Value.Interval(1, 2, 3))?
	conn.close()?

test "a cached statement sends binary parameters and is reused":
	var conn = open(config()?)?
	sql := "select $1::int8 + $2::int4, $3::bytea, $4::text, $5::uuid, $6::timestamptz, $7::date, $8::bool, $9::float8, $10::int2"
	stamp := Timestamp(seconds: 1700000000, nanos: 123456000)
	args := [
		Value.Int(1),
		Value.Int(2),
		Value.Bytes(Bytes([0, 255, 7])),
		Value.Text("héllo"),
		Value.Text("123e4567-e89b-12d3-a456-426614174000"),
		Value.Timestamp(stamp.clone()),
		Value.Date(20000),
		Value.Bool(true),
		Value.Float(2.5),
		Value.Int(7),
	]
	first := conn.query_cached(sql, args)?
	second := conn.query_cached(sql, args)?
	expect_equal(second.rows, first.rows)?
	expect_equal(first.rows[0][0], Value.Int(3))?
	expect_equal(first.rows[0][2], Value.Text("héllo"))?
	expect_equal(first.rows[0][1], Value.Bytes(Bytes([0, 255, 7])))?
	expect_equal(first.rows[0][3], Value.Text("123e4567-e89b-12d3-a456-426614174000"))?
	expect_equal(first.rows[0][4], Value.Timestamp(stamp))?
	expect_equal(first.rows[0][5], Value.Date(20000))?
	expect_equal(conn.statements.length(), 1)?

	// An out of range integer is sent as text, so the server reports the error.
	failure := expect_error(conn.query_cached("select $1::int2", [Value.Int(100000)]))?
	expect_equal(failure.kind, "22003")?

	prepared := conn.prepare("gone", "select 1")?
	conn.deallocate(prepared)?
	void expect_error(conn.run(prepared, []))?
	conn.close()?

test "a pool runs a transaction on one connection and rolls back on failure":
	var setup = open(config()?)?
	void setup.simple("drop table if exists pool_tx")?
	void setup.simple("create table pool_tx (n int)")?

	var pool = start(config()?, 2)?
	done := pool.transaction(
		[
			Statement("insert into pool_tx values (1)"),
			Statement("insert into pool_tx values (2)"),
		]
	)?
	expect_equal(done.length(), 2)?
	void expect_error(
		pool.transaction(
			[
				Statement("insert into pool_tx values (3)"),
				Statement("select 1/0"),
			]
		)
	)?

	count := pool.query("select count(*) from pool_tx")?
	expect_equal(count.rows[0][0].int().get(0), 2)?

	piped := pool.batch([Statement("select 1"), Statement("select 2")])?
	expect_equal(piped[1].rows[0][0].int().get(0), 2)?
	pool.ping()?
	pool.close()?
	void setup.simple("drop table pool_tx")?
	setup.close()?

test "a pool replaces a connection past its lifetime and one that was killed":
	var pool = start_with(config()?, 1, PoolOptions(max_lifetime: 1.milliseconds()))?
	first := pool.query("select pg_backend_pid()")?
	sleep(20.milliseconds()).wait()?

	second := pool.query("select pg_backend_pid()")?
	expect_true(first.rows[0][0] != second.rows[0][0])?
	pool.close()?

	var killed = start(config()?, 1)?
	void killed.query("select pg_terminate_backend(pg_backend_pid())")

	var recovered = false

	for _attempt in 0..3:
		if killed.query("select 1").is_ok():
			recovered = true
			break

	expect_true(recovered)?
	killed.close()?

test "connection strings list several hosts":
	parsed := parse_config(
		"postgres://a:5001,b,[::1]:5003/db?target_session_attrs=read-write"
	)?
	expect_equal(parsed.host, "a")?
	expect_equal(parsed.port, 5001)?
	expect_equal(parsed.fallbacks.length(), 2)?
	expect_equal(parsed.fallbacks[0].host, "b")?
	expect_equal(parsed.fallbacks[0].port, 5001)?
	expect_equal(parsed.fallbacks[1].host, "::1")?
	expect_equal(parsed.fallbacks[1].port, 5003)?
	expect_equal(parsed.target_session_attrs, "read-write")?

	words := parse_config("host=a,b port=1,2 dbname=x")?
	expect_equal(words.fallbacks[0].port, 2)?
	void expect_error(parse_config("host=a target_session_attrs=bogus"))?

test "open falls back to the next server and honours the wanted kind":
	good := config()?
	var down = good.clone()
	down.host = "127.0.0.1"
	down.port = 1
	down.fallbacks = [Endpoint(good.host.clone(), good.port)]

	var conn = open(down)?
	probe := conn.query("select 1", [])?
	expect_equal(probe.rows[0][0].int().get(0), 1)?
	conn.close()?

	var writer = good.clone()
	writer.target_session_attrs = "read-write"

	var primary = open(writer)?
	primary.close()?

	var standby = good.clone()
	standby.target_session_attrs = "standby"
	void expect_error(open(standby))?

	var preferred = good.clone()
	preferred.target_session_attrs = "prefer-standby"

	var fallback = open(preferred)?
	fallback.close()?

test "trace events record each statement":
	var settings = config()?
	settings.trace = true

	var conn = open(settings)?
	void conn.take_trace()
	void conn.query("select $1::int4", [Value.Int(1)])?
	void expect_error(conn.simple("select 1/0"))?

	events := conn.take_trace()
	expect_equal(events.length(), 2)?
	expect_equal(events[0].kind, "query")?
	expect_true(events[0].failure.is_none())?
	expect_equal(events[1].sql, "select 1/0")?
	expect_equal(events[1].failure, "22012")?
	expect_true(conn.take_trace().is_empty())?
	conn.close()?

test "row helpers pick columns and single rows":
	var conn = open(config()?)?
	rows := conn.query(
		"select 1 as a, 'x' as b union all select 2, 'y' order by 1",
		[],
	)?
	expect_equal(rows.column_index("b"), 1)?
	expect_equal(rows.column("a")?, [Value.Int(1), Value.Int(2)])?
	expect_equal(rows.maps()[1].get_or("b", Value.Null), Value.Text("y"))?

	many := expect_error(rows.one())?
	expect_equal(many.kind, "ETOOMANY")?

	none := conn.query("select 1 where false", [])?
	missing := expect_error(none.one())?
	expect_equal(missing.kind, "ENOROWS")?

	single := conn.query("select 5", [])?
	only := single.one()?
	expect_equal(only[0], Value.Int(5))?
	conn.ping()?
	expect_true(conn.parameter("server_version").is_some())?
	expect_equal(conn.in_transaction(), false)?
	conn.close()?

test "large objects are written, read, sought and removed":
	var conn = open(config()?)?

	void conn.transaction(
		fn(var c: Conn):
			oid := lo_create(var c, 0)?
			fd := lo_open(var c, oid, LoMode.ReadWrite)?
			expect_equal(lo_write(var c, fd, Bytes([104, 101, 108, 108, 111]))?, 5)?
			expect_equal(lo_seek(var c, fd, 1, Whence.Start)?, 1)?
			expect_equal(lo_read(var c, fd, 3)?, Bytes([101, 108, 108]))?
			expect_equal(lo_tell(var c, fd)?, 4)?
			lo_truncate(var c, fd, 2)?
			expect_equal(lo_seek(var c, fd, 0, Whence.End)?, 2)?
			lo_close(var c, fd)?
			lo_unlink(var c, oid)?
			return ok(0)
	)?

	conn.close()?

fn add_total(var total: Int, row: List[Value]) Void!:
	total += row[0].int().get(0)
	return ok()

fn refuse(row: List[Value]) Void!:
	return error(Error("stop", kind: "ESTOP"))

test "a pool streams rows from a pooled connection":
	var pool = start(config()?, 2)?
	var total = 0
	header := pool.each(
		"select n from generate_series(1, 20000) n",
		[],
		fn row: add_total(var total, row)
	)?
	expect_equal(total, 200010000)?
	expect_equal(header.columns, ["n"])?
	expect_equal(header.command, "SELECT 20000")?

	// A failing callback does not leave the worker out of step.
	failure := expect_error(
		pool.each(
			"select n from generate_series(1, 500) n",
			[],
			fn row: refuse(row)
		)
	)?
	expect_equal(failure.kind, "ESTOP")?

	after := pool.query("select 7::int")?
	expect_equal(after.rows[0][0], Value.Int(7))?

	// The same connection serves a server error.
	broken := expect_error(pool.each("select 1/0", [], fn row: refuse(row)))?
	expect_equal(broken.kind, "22012")?
	pool.close()?

fn text_conn() Conn!:
	cfg := config()?
	return open(
		Config(
			host: cfg.host.clone(),
			port: cfg.port,
			user: cfg.user.clone(),
			password: cfg.password.clone(),
			binary_results: false,
		)
	)

test "ranges, geometry, network, bit and time types agree in both formats":
	var binary = open(config()?)?
	var text = text_conn()?
	sql := "select '[1,5)'::int4range, '(,3]'::int4range, 'empty'::int4range, '{[1,3),[5,7)}'::int4multirange, '[2026-01-01,2026-02-01)'::daterange, '[1.5,2.5]'::numrange, '[1,3]'::int8range, '(1,2)'::point, '((0,0),(1,1))'::box, '[(0,0),(1,1)]'::path, '((0,0),(1,1))'::path, '((0,0),(1,0),(1,1))'::polygon, '{1,2,3}'::line, '<(1,2),3>'::circle, '[(0,0),(1,1)]'::lseg, '10.1.2.3'::inet, '10.0.0.0/8'::cidr, '::1'::inet, '2001:db8::1/64'::inet, '1010'::bit(4), '10101'::varbit, '12:34:56.789'::time, '12:34:56+05:30'::timetz, '08:00:2b:01:02:03'::macaddr, array['[1,3)'::int4range, '[5,7)']"
	typed := first_row(var binary, sql)?
	expect_equal(typed, first_row(var text, sql)?)?
	expect_equal(typed[0], Value.Range([Value.Int(1), Value.Int(5)], 1))?
	expect_equal(typed[1], Value.Range([Value.Null, Value.Int(4)], 0))?
	expect_equal(typed[2], Value.Range([Value.Null, Value.Null], 4))?
	expect_equal(typed[3].array().get([]).length(), 2)?
	expect_equal(typed[7], Value.Geometric("point", [1.0, 2.0], false))?
	expect_equal(typed[13], Value.Geometric("circle", [1.0, 2.0, 3.0], false))?
	expect_equal(typed[15], Value.Inet("10.1.2.3", 32))?
	expect_equal(typed[16], Value.Inet("10.0.0.0", 8))?
	expect_equal(typed[17], Value.Inet("::1", 128))?
	expect_equal(typed[18], Value.Inet("2001:db8::1", 64))?
	expect_equal(typed[19], Value.Bits(4, Bytes([160])))?
	expect_equal(typed[20], Value.Bits(5, Bytes([168])))?
	expect_equal(typed[21], Value.Time(45296789000))?
	expect_equal(typed[22], Value.TimeTz(45296000000, 19800))?
	expect_equal(typed[23], Value.Text("08:00:2b:01:02:03"))?
	binary.close()?
	text.close()?

test "ranges and geometry go out as parameters":
	var conn = open(config()?)?
	rows := conn.query(
		"select $1::int4range, $2::point, $3::inet, $4::bit(3), $5::time",
		[
			Value.Range([Value.Int(1), Value.Int(9)], 1),
			Value.Geometric("point", [1.5, -2.0], false),
			Value.Inet("192.168.0.1", 24),
			Value.Bits(3, Bytes([160])),
			Value.Time(3723000000),
		],
	)?
	expect_equal(rows.rows[0][0], Value.Range([Value.Int(1), Value.Int(9)], 1))?
	expect_equal(rows.rows[0][1], Value.Geometric("point", [1.5, -2.0], false))?
	expect_equal(rows.rows[0][2], Value.Inet("192.168.0.1", 24))?
	expect_equal(rows.rows[0][3], Value.Bits(3, Bytes([160])))?
	expect_equal(rows.rows[0][4], Value.Time(3723000000))?
	conn.close()?

test "enums, composites, domains, ranges and hstore load by name":
	var conn = open(config()?)?
	void conn.simple("create extension if not exists hstore")?
	void conn.simple("drop type if exists pg_pair, pg_mood cascade")?
	void conn.simple("drop domain if exists pg_posint")?
	void conn.simple("drop type if exists pg_span cascade")?
	void conn.simple("create type pg_mood as enum ('sad', 'ok')")?
	void conn.simple("create type pg_pair as (n int4, note text, mood pg_mood)")?
	void conn.simple("create domain pg_posint as int4 check (value > 0)")?
	void conn.simple("create type pg_span as range (subtype = int4)")?
	conn.load_types(["pg_mood", "pg_pair", "pg_posint", "pg_span", "hstore"])?
	sql := "select 'ok'::pg_mood, row(1, 'a b', 'sad')::pg_pair, 5::pg_posint, '[1,4)'::pg_span, 'a=>1, b=>NULL'::hstore, array['sad', 'ok']::pg_mood[], array[row(2, null, 'ok')]::pg_pair[]"
	var binary_conn = conn
	typed := first_row(var binary_conn, sql)?
	expect_equal(typed[0], Value.Text("ok"))?
	expect_equal(typed[1], Value.Composite([Value.Int(1), Value.Text("a b"), Value.Text("sad")]))?
	expect_equal(typed[2], Value.Int(5))?
	expect_equal(typed[3], Value.Range([Value.Int(1), Value.Int(4)], 1))?
	expect_equal(typed[4], Value.Hstore(["a", "b"], [Value.Text("1"), Value.Null]))?
	expect_equal(typed[5], Value.Array([Value.Text("sad"), Value.Text("ok")]))?
	expect_equal(typed[6], Value.Array([Value.Composite([Value.Int(2), Value.Null, Value.Text("ok")])]))?
	queried := binary_conn.query(sql, [])?
	expect_equal(queried.rows[0], typed)?
	void binary_conn.simple("drop type pg_pair, pg_mood, pg_span cascade")?
	void binary_conn.simple("drop domain pg_posint")?
	binary_conn.close()?

test "named arguments and simple protocol substitution skip quoted text":
	var args = Map[String, Value]()
	args.add("id", Value.Int(7))
	args.add("name", Value.Text("o'brien"))
	rewritten := named("select @id, '@id', @name, @id -- @id\n, @@x", args)?
	expect_equal(rewritten.sql, "select $1, '@id', $2, $1 -- @id\n, @x")?
	expect_equal(rewritten.params.length(), 2)?
	void expect_error(named("select @missing", args))?
	text := interpolate("select $1, '$1', $2, $3, $4", [Value.Int(1), Value.Text("it's"), Value.Null, Value.Bool(true)])?
	expect_equal(text, "select 1, '$1', 'it''s', NULL, true")?
	void expect_error(interpolate("select $2", [Value.Int(1)]))?

test "named, simple and binary COPY paths agree with the extended protocol":
	var conn = open(config()?)?
	var args = Map[String, Value]()
	args.add("n", Value.Int(41))
	named_rows := conn.query_named("select @n::int8 + 1", args)?
	expect_equal(named_rows.rows[0][0], Value.Int(42))?
	simple_rows := conn.query_simple("select $1::int8 + 1, $2::text", [Value.Int(41), Value.Text("a'b")])?
	expect_equal(simple_rows.rows[0][1], Value.Text("a'b"))?
	expect_equal(simple_rows.verb(), "SELECT")?

	void conn.simple("create temp table bc (id int8, note text, at timestamptz, raw bytea, ok bool)")?
	stamp := Timestamp(seconds: 1700000000, nanos: 0)
	rows := [
		[Value.Int(1), Value.Text("a"), Value.Timestamp(stamp.clone()), Value.Bytes(Bytes([0, 1])), Value.Bool(true)],
		[Value.Int(2), Value.Null, Value.Null, Value.Null, Value.Bool(false)],
	]
	stored := conn.copy_from_binary(["bc"], [], from_list(rows))?
	expect_equal(stored, 2)?
	back := conn.query("select id, note, at, raw, ok from bc order by id", [])?
	expect_equal(back.rows[0][2], Value.Timestamp(stamp))?
	expect_equal(back.rows[1][1], Value.Null)?
	void expect_error(conn.copy_from_binary(["bc"], ["id"], from_list([[Value.Text("x")]])))?
	conn.close()?
