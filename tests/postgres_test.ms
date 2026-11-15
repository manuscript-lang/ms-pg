import (
	Bytes from "@std/bytes.ms",
	Promise, promise from "@std/concurrency/promise.ms",
	collect, filter, fold, from_list, map, take from "@std/io/streams.ms",
	Connection, listen from "@std/net/net.ms",
	TlsOptions from "@std/net/tls.ms",
	environment from "@std/os/env.ms",
	read_file from "@std/os/fs.ms",
	expect_equal, expect_error, expect_true from "@std/test/test.ms",
	Timestamp, sleep from "@std/time.ms",

	md5_password from "@pg/auth.ms",
	Batch, Config, Conn, Isolation, TxOptions, cancel, open from "@pg/conn.ms",
	config_from_env, parse_config from "@pg/dsn.ms",
	Pool, Statement, start from "@pg/pool.ms",
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

test "binary and text results agree, and numeric stays text":
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
	expect_equal(row[3], Value.Text("1.25"))?

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
