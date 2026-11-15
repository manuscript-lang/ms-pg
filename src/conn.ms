// One PostgreSQL connection: startup, authentication, queries and transactions.
import (
	Bytes, BytesBuilder from "@std/bytes.ms",
	ReadStream from "@std/io/io.ms",
	from_list from "@std/io/streams.ms",
	Connection, connect, connect_unix, start_tls from "@std/net/net.ms",
	TlsOptions from "@std/net/tls.ms",
	Duration, Instant, monotonic from "@std/time.ms",

	md5_password, scram_start from "@pg/auth.ms",
	Param, TypeInfo, decode_value, encode_param, is_binary_type from "@pg/decode.ms",
	interpolate, named from "@pg/sql.ms",
	Value, encode_value from "@pg/value.ms",
	Backend,
	Field,
	PORTAL,
	STATEMENT,
	STATUS_IDLE,
	bind,
	cancel_request,
	close_statement,
	copy_data,
	copy_done,
	copy_fail,
	decode_message,
	describe,
	execute,
	i32,
	parse,
	password,
	query,
	sasl_initial,
	sasl_response,
	ssl_request,
	startup,
	sync,
	terminate from "@pg/wire.ms",
)

// Another server to try when the first is down or is not the kind asked for.
export type Endpoint:
	host: String
	port: Int = 5432

// A host that starts with `/` is a Unix socket directory (`/var/run/postgresql`), and
// the socket is `.s.PGSQL.<port>` inside it. TLS does not apply to a Unix socket.
// How to reach the server and which logins to accept.
export type Config:
	host: String = "127.0.0.1"
	port: Int = 5432
	user: String = "postgres"
	password: String = ""
	database: String = "postgres"
	application_name: String = "manuscript"
	// TLS negotiated with an SSLRequest on the plain connection, then the same socket
	// is upgraded; none is plain TCP.
	tls: TlsOptions? = none
	// Opens a replication connection: `database` for logical replication, `true` for
	// physical. See `@pg/replication.ms`.
	replication: String = ""
	// Accept an md5 password challenge. Off by default: md5 is weak, and SCRAM-SHA-256
	// is always accepted.
	allow_md5: Bool = false
	timeout: Duration? = none
	// Fetch the columns of prepared statements in binary format where a decoder exists
	// (integers, floats, bool, bytea, text, uuid, date, timestamp). Other columns, such
	// as numeric, stay text. Off gives text for everything. The format only changes
	// speed: every call path returns the same `Value` variants.
	binary_results: Bool = true
	// Extra server settings sent at startup, such as `search_path` or
	// `statement_timeout`.
	params: Map[String, String] = Map[String, String]()
	// How many statements `query_cached` keeps prepared before it clears them all. Zero
	// turns the cache off, and every call parses again.
	statement_cache: Int = 512
	// Servers tried in order after `host` and `port`, as in a connection string with
	// `host=a,b`.
	fallbacks: List[Endpoint] = []
	// Which server `open` accepts: `any`, `read-write`, `read-only`, `primary`,
	// `standby` or `prefer-standby` (a standby when one answers, otherwise any).
	target_session_attrs: String = "any"
	// Record a `TraceEvent` for each statement; read them with `take_trace`.
	trace: Bool = false

// The result of one statement.
export type Rows:
	columns: List[String]
	rows: List[List[Value]]
	// The command tag, such as `SELECT 3` or `INSERT 0 1`.
	command: String

// The rows as a stream, so a result composes with the stream steps:
// `rows.stream() | filter(fn row: ...) | map(fn row: ...) | collect(max_items: 100)`.
// The stream owns a copy of the rows.
fn Rows.stream() ReadStream[List[Value]]:
	return from_list(self.rows.clone())

// Rows changed, from a command tag such as `UPDATE 2`; none when the tag carries no
// count.
fn Rows.affected() Int?:
	parts := self.command.split(" ")
	return parts[parts.length() - 1].parse_int()

// An open connection. One owner at a time; use a `Pool` to share.
export type Conn:
	net: Connection
	buffer: Bytes
	pos: Int
	parameters: Map[String, String]
	pid: Int
	secret: Int
	// The transaction status of the last ReadyForQuery: `STATUS_IDLE`,
	// `STATUS_IN_TRANSACTION` or `STATUS_FAILED`.
	status: UInt8
	binary_results: Bool
	// Statements `query_cached` parsed, by SQL text.
	statements: Map[String, Prepared]
	statement_cache: Int
	// Types registered by `load_types`, by OID.
	types: Map[Int, TypeInfo]
	trace: Bool
	events: List[TraceEvent]
	// NOTIFY messages that arrived while a statement was running, oldest first.
	notifications: List[Notification]

// One finished statement, kept when `Config.trace` is on: what ran (`query`, `simple`,
// `run` or `batch`), the SQL, how long it took and the error kind if it failed.
export type TraceEvent:
	kind: String
	sql: String
	elapsed: Duration
	failure: String?

// One NOTIFY: the sending backend's process id, the channel and the payload.
export type Notification:
	pid: Int
	channel: String
	payload: String

fn server_error(fields: Map[String, String]) Error:
	var details = Map[String, String]()
	for entry in fields: details.add(entry.key.clone(), entry.value.clone())
	return Error(
		fields.get_or("M", "server error"),
		kind: fields.get_or("C", "XX000"),
		details: details,
	)

fn protocol(what: String) Error:
	return Error(f"postgres: {what}", kind: "EPROTO")

// Makes at least `count` unread bytes available, joining chunks once so a large message
// costs linear time.
fn var Conn.need(count: Int) Void!:
	if self.buffer.length() - self.pos >= count: return ok()

	var joined = BytesBuilder()
	joined.append_bytes(self.buffer.slice(self.pos, self.buffer.length())?)

	var total = self.buffer.length() - self.pos

	while total < count:
		chunk := self.net.input.next()?
		if chunk.is_none(): return error(
			Error(
				"postgres: connection closed by server",
				kind: "ECONNRESET",
			)
		)

		piece := chunk.get(Bytes([]))
		total += piece.length()
		joined.append_bytes(piece)

	self.buffer = joined.build()
	self.pos = 0
	return ok()

// Reads one framed message exactly as the server sent it.
fn var Conn.frame() Backend!:
	self.need(5)?

	code := self.buffer.uint8(self.pos)?
	size := i32(self.buffer, self.pos + 1)?
	if size < 4: return error(protocol("invalid message length"))

	self.need(size + 1)?

	body := self.buffer.slice(self.pos + 5, self.pos + 1 + size)?
	self.pos += 1 + size
	return decode_message(code, body)

// Reads the next message, skipping notices, parameter changes and ignorable messages,
// and queueing notifications. A server error arrives as `ErrorResponse`, since the
// server keeps the stream in step after it.
fn var Conn.read() Backend!:
	while true:
		message := self.frame()?

		match message:
			Backend.Notification(pid, channel, payload):
				self.notifications.append(
					Notification(
						pid,
						channel.clone(),
						payload.clone(),
					)
				)
				continue
			Backend.NoticeResponse(_): continue
			Backend.Ignored: continue
			Backend.ParameterStatus(name, value):
				self.parameters.add(name.clone(), value.clone())
				continue
			_: return ok(message)

	return error(protocol("unreachable"))

fn var Conn.send(data: Bytes) Void!:
	self.net.output.write(data)?

// Asks for TLS on the plain connection (SSLRequest), then upgrades the same socket. The
// server must answer with the single byte `S`; anything more would be plaintext
// injected before the handshake.
fn negotiate_tls(var net: Connection, tls: TlsOptions, host: String, timeout: Duration?) Connection!:
	net.output.write(ssl_request())?

	reply := net.input.next()?
	if reply.is_none(): return error(
		Error("postgres: connection closed by server", kind: "ECONNRESET")
	)

	answer := reply.get(Bytes([]))
	if answer.length() != 1: return error(
		protocol("unexpected bytes after the SSL request")
	)
	if answer.get(0) == 78: return error(
		Error("postgres: server does not support TLS", kind: "ENOTSUP")
	)
	if answer.get(0) != 83: return error(
		protocol("unexpected answer to the SSL request")
	)

	var options = tls.clone()
	if options.server_name.is_empty(): options.server_name = host.clone()
	return start_tls(var net, options, timeout)

// Opens the socket, and upgrades it to TLS when configured and the host is not a Unix
// socket.
fn dial(config: Config) Connection!:
	if config.host.starts_with("/"):
		return connect_unix(
			f"{config.host}/.s.PGSQL.{config.port}",
			config.timeout.clone(),
		).wait()

	var net = connect(f"{config.host}:{config.port}", none, config.timeout.clone()).wait()?
	if options := config.tls:
		net = negotiate_tls(var net, options, config.host, config.timeout.clone())?
	return ok(net)

// Asks the server to stop the statement a connection is running. Call it from another
// task with the target's `pid` and `secret`, on a fresh connection that the server
// closes at once; the target sees a `57014` error. The server sends no reply, so this
// returns when the request is written.
export fn cancel(config: Config, pid: Int, secret: Int) Void!:
	var net = dial(config)?
	net.output.write(cancel_request(pid, secret)?)?
	return net.close()

fn var Conn.attrs_match(wanted: String) Bool!:
	if wanted == "any": return ok(true)

	if wanted == "read-write" or wanted == "read-only":
		mode := self.simple("show transaction_read_only")?
		off := mode.rows[0][0].text().get("") == "off"
		return ok(off == (wanted == "read-write"))

	recovery := self.simple("select pg_is_in_recovery()")?
	standby := recovery.rows[0][0].bool().get(false)
	return ok(standby == (wanted == "standby" or wanted == "prefer-standby"))

fn refuse_gss(_token: Bytes?) Bytes!:
	return error(
		Error("postgres: server asked for GSSAPI; use open_gss", kind: "EACCES")
	)

// Like `open` for a server that asks for GSSAPI or SSPI: `gss` is called with the
// server's token (none at the start) and returns the token to send back, which may be
// empty. Kerberos itself comes from the application's library, as in pgx. It opens
// `host` only, with no failover.
export fn open_gss(config: Config, gss: fn(Bytes?) Bytes!) Conn!:
	return open_endpoint(config, gss)

// Opens one server of `config` and checks it is the kind asked for.
fn open_server(config: Config, wanted: String) Conn!:
	var conn = open_endpoint(config, refuse_gss)?
	if conn.attrs_match(wanted)?: return ok(conn)

	void conn.close()
	return error(
		Error(
			f"postgres: {config.host}:{config.port} is not {wanted}",
			kind: "ENOTSUP",
		)
	)

// Connects, authenticates, and returns a connection ready for queries. Servers are
// tried in order (`host`, then `fallbacks`); the first that answers and matches
// `target_session_attrs` wins, and the last failure is returned when none does.
export fn open(config: Config) Conn!:
	var servers = [Endpoint(config.host.clone(), config.port)]
	for server in config.fallbacks: servers.append(server.clone())

	var wanted = config.target_session_attrs.clone()
	var passes = 1
	if wanted == "prefer-standby": passes = 2

	var last = Error("postgres: no server to try", kind: "EINVAL")

	for pass in 0..passes:
		if pass == 1: wanted = "any"

		for server in servers:
			var attempt = config.clone()
			attempt.host = server.host.clone()
			attempt.port = server.port
			attempt.fallbacks = []

			opened := open_server(attempt, wanted)
			if opened.is_ok(): return opened

			last = opened.err().clone()

	return error(last)

fn open_endpoint(config: Config, gss: fn(Bytes?) Bytes!) Conn!:
	var net = dial(config)?
	var conn = Conn(
		net,
		Bytes([]),
		0,
		Map[String, String](),
		0,
		0,
		STATUS_IDLE,
		config.binary_results,
		Map[String, Prepared](),
		config.statement_cache,
		Map[Int, TypeInfo](),
		config.trace,
		[],
		[],
	)
	var parameters = config.params.clone()
	parameters.add("application_name", config.application_name.clone())
	if not config.replication.is_empty(): parameters.add(
		"replication",
		config.replication.clone(),
	)
	conn.send(startup(config.user, config.database, parameters)?)?
	conn.authenticate(config, gss)?
	return ok(conn)

fn var Conn.authenticate(config: Config, gss: fn(Bytes?) Bytes!) Void!:
	var scram = scram_start(config.password.clone())?
	var in_scram = false
	var server_verified = false

	while true:
		message := self.read()?

		match message:
			Backend.AuthOk:
				if in_scram and not server_verified:
					return error(
						Error(
							"postgres: server did not prove it knows the password",
							kind: "EACCES",
						)
					)

				break
			Backend.AuthCleartext:
				if config.tls.is_none():
					return error(
						Error(
							"postgres: refusing a cleartext password without TLS",
							kind: "EACCES",
						)
					)
				self.send(password(config.password)?)?
			Backend.AuthMd5(salt):
				if not config.allow_md5:
					return error(
						Error(
							"postgres: server asked for md5; set allow_md5 to accept it",
							kind: "EACCES",
						)
					)
				self.send(password(md5_password(config.user, config.password, salt))?)?
			Backend.AuthGss:
				self.send(sasl_response(gss(none)?)?)?
			Backend.AuthGssContinue(data):
				token := gss(data.clone())?
				if token.length() > 0: self.send(sasl_response(token)?)?
			Backend.AuthSasl(mechanisms):
				if not mechanisms.contains("SCRAM-SHA-256"):
					return error(protocol("server offers no SCRAM-SHA-256"))

				in_scram = true
				self.send(sasl_initial("SCRAM-SHA-256", scram.first_message())?)?
			Backend.AuthSaslContinue(data):
				text := data.decode_utf8().ok_or(
					protocol("SCRAM message is not UTF-8")
				)?
				self.send(sasl_response(scram.respond(text)?)?)?
			Backend.AuthSaslFinal(data):
				text := data.decode_utf8().ok_or(
					protocol("SCRAM message is not UTF-8")
				)?
				scram.verify(text)?
				server_verified = true
			Backend.ErrorResponse(fields): return error(server_error(fields))
			_: return error(protocol("unexpected message during login"))

	while true:
		message := self.read()?

		match message:
			Backend.BackendKeyData(pid, secret):
				self.pid = pid
				self.secret = secret
			Backend.ReadyForQuery(status):
				self.status = status
				return ok()
			Backend.ErrorResponse(fields): return error(server_error(fields))
			_: continue

	return ok()

// Reads messages up to ReadyForQuery, handing each decoded row to `emit` as it arrives.
// A server error or a value that cannot be decoded is remembered, and the first one is
// returned once the server has finished, so the connection stays usable. A failed read
// or a failed `emit` ends the connection's use and returns at once. The result holds
// the column names and command tag, with no rows.
fn var Conn.pump(known: List[Field], emit: fn(List[Value]) Void!) Rows!:
	var fields = known.clone()
	var command = ""
	var failure: Error? = none

	while true:
		message := self.read()?

		match message:
			Backend.RowDescription(found): fields = found.clone()
			Backend.DataRow(columns):
				var row = List[Value]()

				for i in 0..columns.length():
					if i >= fields.length():
						if failure.is_none(): failure = protocol(
							"row has more columns than its description"
						)
						break

					decoded := decode_value(
						fields[i].type_oid,
						fields[i].format,
						columns[i].clone(),
						self.types,
					)

					if decoded.is_error():
						if failure.is_none(): failure = decoded.err().clone()
						break

					row.append(decoded?)

				if failure.is_none(): emit(row)?
			Backend.CommandComplete(tag): command = tag.clone()
			Backend.ErrorResponse(found):
				if failure.is_none(): failure = server_error(found)
			Backend.ReadyForQuery(status):
				self.status = status
				break
			_: continue

	if f := failure: return error(f)

	return ok(Rows(fields.map(fn f: f.name.clone()), [], command))

fn append_row(var rows: List[List[Value]], row: List[Value]) Void!:
	rows.append(row)
	return ok()

// Reads one statement's reply into memory.
fn var Conn.collect(known: List[Field]) Rows!:
	var rows = List[List[Value]]()
	header := self.pump(known, fn row: append_row(var rows, row))?
	return ok(Rows(header.columns.clone(), rows, header.command.clone()))

// Runs a statement with no parameters over the simple protocol; may hold several
// statements.
fn var Conn.simple(sql: String) Rows!:
	started := monotonic()
	result := self.simple_untraced(sql)
	self.record("simple", sql, started, result.error())
	return result

fn var Conn.simple_untraced(sql: String) Rows!:
	self.send(query(sql)?)?
	return self.collect([])

// Runs one statement with parameters in a single round trip, handing each row to `emit`
// as it arrives instead of collecting them.
fn var Conn.run_streaming(
	sql: String,
	params: List[Value],
	emit: fn(List[Value]) Void!
) Rows!:
	var out = BytesBuilder()
	out.append_bytes(parse("", sql, [])?)
	out.append_bytes(bind("", "", params.map(fn p: encode_value(p)), [], [])?)
	out.append_bytes(describe(PORTAL, "")?)
	out.append_bytes(execute("", 0)?)
	out.append_bytes(sync()?)
	self.send(out.build())?
	return self.pump([], emit)

// Runs one statement with parameters in a single round trip.
fn var Conn.query(sql: String, params: List[Value]) Rows!:
	started := monotonic()
	result := self.query_untraced(sql, params)
	self.record("query", sql, started, result.error())
	return result

fn var Conn.query_untraced(sql: String, params: List[Value]) Rows!:
	var out = BytesBuilder()
	out.append_bytes(parse("", sql, [])?)
	out.append_bytes(bind("", "", params.map(fn p: encode_value(p)), [], [])?)
	out.append_bytes(describe(PORTAL, "")?)
	out.append_bytes(execute("", 0)?)
	out.append_bytes(sync()?)
	self.send(out.build())?
	return self.collect([])

fn var Conn.begin() Void!:
	void self.simple("BEGIN")?
	return ok()

fn var Conn.commit() Void!:
	void self.simple("COMMIT")?
	return ok()

fn var Conn.rollback() Void!:
	void self.simple("ROLLBACK")?
	return ok()

fn var Conn.close() Void!:
	void self.send(terminate()?)
	return self.net.close()

// A statement parsed once on the server and run many times.
export type Prepared:
	name: String
	// The result columns, each with the format code the statement requests for it.
	columns: List[Field]
	// The type OID of each parameter, which picks text or binary encoding in `run`.
	param_oids: List[Int]
	sql: String

// Parses `sql` under `name` and learns its result columns.
fn var Conn.prepare(name: String, sql: String) Prepared!:
	var out = BytesBuilder()
	out.append_bytes(parse(name, sql, [])?)
	out.append_bytes(describe(STATEMENT, name)?)
	out.append_bytes(sync()?)
	self.send(out.build())?

	var columns = List[Field]()
	var param_oids = List[Int]()
	var failure: Error? = none

	while true:
		message := self.read()?

		match message:
			Backend.RowDescription(found): columns = found.clone()
			Backend.ParameterDescription(oids): param_oids = oids.clone()
			Backend.ErrorResponse(found):
				if failure.is_none(): failure = server_error(found)
			Backend.ReadyForQuery(status):
				self.status = status
				break
			_: continue

	if f := failure: return error(f)

	if self.binary_results:
		columns = columns.map(
			fn c: Field(
				c.name.clone(),
				c.type_oid,
				if is_binary_type(c.type_oid, self.types): 1 else: 0,
			)
		)
	return ok(Prepared(name.clone(), columns, param_oids, sql.clone()))

// Runs a prepared statement: bind, execute and sync in one round trip.
fn var Conn.run(statement: Prepared, params: List[Value]) Rows!:
	started := monotonic()
	result := self.run_untraced(statement, params)
	self.record("run", statement.sql, started, result.error())
	return result

fn var Conn.run_untraced(statement: Prepared, params: List[Value]) Rows!:
	var out = BytesBuilder()
	formats := statement.columns.map(fn c: c.format)
	var encoded = List[Param]()

	for i in 0..params.length():
		if self.binary_results and i < statement.param_oids.length():
			encoded.append(encode_param(statement.param_oids[i], params[i]))
		else:
			encoded.append(Param(encode_value(params[i]), 0))

	out.append_bytes(
		bind(
			"",
			statement.name,
			encoded.map(fn p: p.data.clone()),
			encoded.map(fn p: p.format),
			formats,
		)?
	)
	out.append_bytes(execute("", 0)?)
	out.append_bytes(sync()?)
	self.send(out.build())?
	return self.collect(statement.columns)

// Runs `body` in a transaction: commits when it returns a value, rolls back when it
// returns an error (the body's error is returned, whatever the rollback reports).
fn var Conn.transaction[T](body: fn(var Conn) T!) T!:
	self.begin()?

	result := body(var self)

	if result.is_error():
		void self.rollback()
		return result

	self.commit()?
	return result

// Transaction isolation levels.
export enum Isolation:
	Default
	ReadUncommitted
	ReadCommitted
	RepeatableRead
	Serializable

// How a transaction starts: its isolation level, and whether it may write. `deferrable`
// applies only to a serializable, read-only transaction.
export type TxOptions:
	isolation: Isolation = Isolation.Default
	read_only: Bool = false
	deferrable: Bool = false

fn isolation_sql(level: Isolation) String:
	match level:
		Isolation.Default: return ""
		Isolation.ReadUncommitted: return " ISOLATION LEVEL READ UNCOMMITTED"
		Isolation.ReadCommitted: return " ISOLATION LEVEL READ COMMITTED"
		Isolation.RepeatableRead: return " ISOLATION LEVEL REPEATABLE READ"
		Isolation.Serializable: return " ISOLATION LEVEL SERIALIZABLE"

// Starts a transaction with options.
fn var Conn.begin_with(options: TxOptions) Void!:
	var sql = f"BEGIN{isolation_sql(options.isolation)}"
	if options.read_only: sql = f"{sql} READ ONLY"
	if options.deferrable: sql = f"{sql} DEFERRABLE"
	void self.simple(sql)?
	return ok()

// Like `transaction`, with options.
fn var Conn.transaction_with[T](options: TxOptions, body: fn(var Conn) T!) T!:
	self.begin_with(options)?

	result := body(var self)

	if result.is_error():
		void self.rollback()
		return result

	self.commit()?
	return result

// Quotes an identifier: a table, column or savepoint name that may hold any character.
export fn quote_ident(name: String) String:
	return f"\"{name.replace("\"", "\"\"")}\""

fn var Conn.savepoint(name: String) Void!:
	void self.simple(f"SAVEPOINT {quote_ident(name)}")?
	return ok()

fn var Conn.release_savepoint(name: String) Void!:
	void self.simple(f"RELEASE SAVEPOINT {quote_ident(name)}")?
	return ok()

fn var Conn.rollback_to(name: String) Void!:
	void self.simple(f"ROLLBACK TO SAVEPOINT {quote_ident(name)}")?
	return ok()

// A nested transaction inside a running one: runs `body` between a savepoint and its
// release, and rolls back to the savepoint, keeping the outer transaction usable, when
// the body returns an error.
fn var Conn.nested[T](name: String, body: fn(var Conn) T!) T!:
	self.savepoint(name)?

	result := body(var self)

	if result.is_error():
		void self.rollback_to(name)
		void self.release_savepoint(name)
		return result

	self.release_savepoint(name)?
	return result

// Starts listening on a channel. Notifications queue on the connection while other
// statements run.
fn var Conn.listen(channel: String) Void!:
	void self.simple(f"LISTEN {quote_ident(channel)}")?
	return ok()

fn var Conn.unlisten(channel: String) Void!:
	void self.simple(f"UNLISTEN {quote_ident(channel)}")?
	return ok()

// Sends a notification to every connection listening on `channel`.
fn var Conn.notify(channel: String, payload: String) Void!:
	void self.query(
		"select pg_notify($1, $2)",
		[Value.Text(channel.clone()), Value.Text(payload.clone())],
	)?
	return ok()

// Returns the next notification, waiting for one when none is queued. It blocks until
// one arrives, so run it in its own task and close the connection to end the wait.
fn var Conn.wait_for_notification() Notification!:
	while true:
		if not self.notifications.is_empty():
			first := self.notifications.get(0).clone()
			self.notifications.remove(0)
			return ok(first)

		message := self.frame()?

		match message:
			Backend.Notification(pid, channel, payload):
				return ok(Notification(pid, channel.clone(), payload.clone()))
			Backend.ParameterStatus(name, value): self.parameters.add(
				name.clone(),
				value.clone(),
			)
			_: continue

	return error(protocol("unreachable"))

// Statements run in one round trip. Build it with `add`, then run it with `send_batch`.
export type Batch:
	sql: List[String]
	params: List[List[Value]]

fn var Batch.add(statement: String, params: List[Value] = []) Void:
	self.sql.append(statement.clone())
	self.params.append(params.clone())

fn Batch.length() Int:
	return self.sql.length()

// Sends every queued statement, then one Sync, and reads one `Rows` per statement. The
// statements run in an implicit transaction: when one fails, the server skips the rest
// and the error is returned, with none of the changes kept. All results use the text
// format.
fn var Conn.send_batch(batch: Batch) List[Rows]!:
	started := monotonic()
	result := self.send_batch_untraced(batch)
	self.record("batch", batch.sql.join("; "), started, result.error())
	return result

fn var Conn.send_batch_untraced(batch: Batch) List[Rows]!:
	var out = BytesBuilder()

	for i in 0..batch.length():
		out.append_bytes(parse("", batch.sql[i], [])?)
		out.append_bytes(
			bind(
				"",
				"",
				batch.params[i].map(fn p: encode_value(p)),
				[],
				[],
			)?
		)
		out.append_bytes(describe(PORTAL, "")?)
		out.append_bytes(execute("", 0)?)

	out.append_bytes(sync()?)
	self.send(out.build())?

	var results = List[Rows]()
	var fields = List[Field]()
	var rows = List[List[Value]]()
	var failure: Error? = none

	while true:
		message := self.read()?

		match message:
			Backend.RowDescription(found): fields = found.clone()
			Backend.DataRow(columns):
				var row = List[Value]()

				for i in 0..columns.length():
					if i >= fields.length():
						if failure.is_none(): failure = protocol(
							"row has more columns than its description"
						)
						break

					decoded := decode_value(
						fields[i].type_oid,
						fields[i].format,
						columns[i].clone(),
						self.types,
					)

					if decoded.is_error():
						if failure.is_none(): failure = decoded.err().clone()
						break

					row.append(decoded?)
				rows.append(row)
			Backend.CommandComplete(tag):
				results.append(
					Rows(
						fields.map(fn f: f.name.clone()),
						rows,
						tag.clone(),
					)
				)
				fields = []
				rows = []
			Backend.EmptyQuery:
				results.append(Rows([], [], ""))
				fields = []
				rows = []
			Backend.ErrorResponse(found):
				if failure.is_none(): failure = server_error(found)
			Backend.ReadyForQuery(status):
				self.status = status
				break
			_: continue

	if f := failure: return error(f)

	return ok(results)

// Escapes one value for COPY's text format; NULL is `\N`.
fn copy_field(value: Value) String:
	text := value.as_text()
	if text.is_none(): return "\\N"

	return text.get("").replace("\\", "\\\\").replace("\t", "\\t").replace(
		"\n",
		"\\n",
	).replace("\r", "\\r")

fn copy_line(row: List[Value]) Bytes:
	return f"{row.map(fn v: copy_field(v)).join("\t")}\n".to_bytes()

// Runs a `COPY ... FROM STDIN` statement: sends `head`, each row through `encode`, then
// `tail`, in chunks of at most 64 KiB, and returns how many rows the server stored.
// When the stream or `encode` fails, the COPY is aborted and nothing is stored.
fn var Conn.copy_in(
	statement: String,
	encode: fn(List[Value]) Bytes!,
	head: Bytes,
	tail: Bytes,
	rows: ReadStream[List[Value]]
) Int!:
	self.send(query(statement)?)?

	var failure: Error? = none

	while true:
		message := self.read()?

		match message:
			Backend.CopyInResponse: break
			Backend.ErrorResponse(found): failure = server_error(found)
			Backend.ReadyForQuery(status):
				self.status = status
				if f := failure: return error(f)

				return error(protocol("COPY did not start"))
			_: continue

	var pending = BytesBuilder()
	pending.append_bytes(head)

	var size = head.length()
	var source = rows
	var cause: Error? = none

	while true:
		next := source.next()

		if next.is_error():
			cause = next.err().clone()
			break

		item := next?
		if item.is_none(): break

		line := encode(item.get([]))

		if line.is_error():
			cause = line.err().clone()
			break

		chunk := line?
		size += chunk.length()
		pending.append_bytes(chunk)

		if size >= 65536:
			self.send(copy_data(pending.drain())?)?
			size = 0

	if cause.is_some():
		self.send(copy_fail("input failed")?)?
	else:
		pending.append_bytes(tail)
		self.send(copy_data(pending.drain())?)?
		self.send(copy_done()?)?

	var tag = ""

	while true:
		message := self.read()?

		match message:
			Backend.CommandComplete(found): tag = found.clone()
			Backend.ErrorResponse(found):
				if failure.is_none(): failure = server_error(found)
			Backend.ReadyForQuery(status):
				self.status = status
				break
			_: continue

	if c := cause: return error(c)
	if f := failure: return error(f)

	return ok(Rows([], [], tag).affected().get(0))

fn copy_target(table: List[String], columns: List[String]) String:
	target := table.map(fn t: quote_ident(t)).join(".")
	if columns.is_empty(): return target

	return f"{target} ({columns.map(fn c: quote_ident(c)).join(", ")})"

// Loads rows into a table with `COPY ... FROM STDIN`, in the text format, and returns
// how many the server stored. `table` is the table's name parts
// (`["public", "items"]`); the names are quoted. Rows come from a stream, so a pipeline
// feeds it: `conn.copy_from(["items"], ["id", "name"], rows | map(...))`.
fn var Conn.copy_from(
	table: List[String],
	columns: List[String],
	rows: ReadStream[List[Value]],
) Int!:
	return self.copy_in(
		f"COPY {copy_target(table, columns)} FROM STDIN",
		fn row: ok(copy_line(row)),
		Bytes([]),
		Bytes([]),
		rows
	)

// One row of binary COPY: a field count, then each field as a length and its bytes.
fn binary_copy_row(oids: List[Int], row: List[Value]) Bytes!:
	if row.length() != oids.length(): return error(
		Error(
			"postgres: COPY row has the wrong number of columns",
			kind: "EINVAL",
		)
	)

	var out = BytesBuilder()
	out.append_int16_be(oids.length().to_int16().get(0))

	for i in 0..oids.length():
		param := encode_param(oids[i], row[i])

		if data := param.data:
			if param.format != 1: return error(
				Error(
					f"postgres: column {i} has no binary encoding for this value; use copy_from",
					kind: "ENOTSUP",
				)
			)

			out.append_int32_be(data.length().to_int32().get(0))
			out.append_bytes(data)
		else:
			out.append_int32_be(-1)

	return ok(out.build())

// Like `copy_from`, in the binary format: faster and exact, but each value must have a
// binary encoding for its column (integers, floats, bool, bytea, text, uuid, date and
// timestamps). The column types come from the server, so the table must exist.
fn var Conn.copy_from_binary(
	table: List[String],
	columns: List[String],
	rows: ReadStream[List[Value]],
) Int!:
	names := if columns.is_empty(): "*" else: columns.map(
		fn c: quote_ident(c)
	).join(", ")
	probe := self.prepare(
		"",
		f"select {names} from {table.map(fn t: quote_ident(t)).join(".")}",
	)?
	oids := probe.columns.map(fn c: c.type_oid)
	var head = BytesBuilder()
	head.append_bytes(Bytes([80, 71, 67, 79, 80, 89, 10, 255, 13, 10, 0]))
	head.append_int32_be(0)
	head.append_int32_be(0)

	var tail = BytesBuilder()
	tail.append_int16_be(-1)
	return self.copy_in(
		f"COPY {copy_target(table, columns)} FROM STDIN (FORMAT binary)",
		fn row: binary_copy_row(oids, row),
		head.build(),
		tail.build(),
		rows
	)

fn unescape_copy(text: String) Value:
	if text == "\\N": return Value.Null

	// Split on an escaped backslash first, so its neighbours are read as plain text.
	parts := text.split("\\\\", none).map(
		fn part: part.replace("\\t", "\t").replace("\\n", "\n").replace(
			"\\r",
			"\r",
		)
	)
	return Value.Text(parts.join("\\"))

// Runs a `COPY ... TO STDOUT` statement, or `COPY (select ...) TO STDOUT`, and returns
// each line as a row of `Text` values (`Null` for NULL), in the text format.
fn var Conn.copy_to(sql: String) Rows!:
	self.send(query(sql)?)?

	var data = BytesBuilder()
	var failure: Error? = none
	var tag = ""

	while true:
		message := self.read()?

		match message:
			Backend.CopyData(chunk): data.append_bytes(chunk)
			Backend.CommandComplete(found): tag = found.clone()
			Backend.ErrorResponse(found):
				if failure.is_none(): failure = server_error(found)
			Backend.ReadyForQuery(status):
				self.status = status
				break
			_: continue

	if f := failure: return error(f)

	text := data.build().decode_utf8().ok_or(
		Error("postgres: COPY output is not valid UTF-8", kind: "EBADMSG")
	)?
	var rows = List[List[Value]]()

	for line in text.split("\n", none):
		if line.is_empty(): continue

		rows.append(line.split("\t", none).map(fn field: unescape_copy(field)))

	return ok(Rows([], rows, tag))

// Frees a prepared statement on the server.
fn var Conn.deallocate(statement: Prepared) Void!:
	var out = BytesBuilder()
	out.append_bytes(close_statement(statement.name)?)
	out.append_bytes(sync()?)
	self.send(out.build())?
	void self.collect([])?
	return ok()

// Runs `sql` through a per-connection statement cache: the first call parses and names
// the statement, later calls with the same text reuse it and send binary parameters
// and results. When the cache is full, it is cleared with DEALLOCATE ALL.
fn var Conn.query_cached(sql: String, params: List[Value]) Rows!:
	if self.statement_cache <= 0: return self.query(sql, params)

	if known := self.statements.get(sql):
		reuse := known.clone()
		return self.run(reuse, params)

	if self.statements.length() >= self.statement_cache:
		void self.simple("DEALLOCATE ALL")?
		self.statements.clear()

	name := f"ms_{self.statements.length()}"
	prepared := self.prepare(name, sql)?
	self.statements.add(sql.clone(), prepared.clone())
	return self.run(prepared, params)

// The events recorded since the last call, oldest first. Empty unless `Config.trace`.
fn var Conn.take_trace() List[TraceEvent]:
	taken := self.events.clone()
	self.events = []
	return taken

fn var Conn.record(kind: String, sql: String, started: Instant, failure: Error?) Void:
	if not self.trace: return

	var failure_kind: String? = none
	if f := failure: failure_kind = f.kind.get("error")
	self.events.append(
		TraceEvent(
			kind,
			sql.clone(),
			monotonic().difference(started),
			failure_kind,
		)
	)

// Round-trips to the server; an error means the connection is unusable.
fn var Conn.ping() Void!:
	void self.simple("")?
	return ok()

// A server setting the server reported, such as `server_version` or `TimeZone`.
fn Conn.parameter(name: String) String?:
	return self.parameters.get(name).map(fn v: v.clone())

fn Conn.in_transaction() Bool:
	return self.status != STATUS_IDLE

// The index of a result column by name.
fn Rows.column_index(name: String) Int?:
	return self.columns.index_of(name)

// One column of every row.
fn Rows.column(name: String) List[Value]!:
	index := self.column_index(
		name
	).ok_or(Error(f"postgres: no column {name}", kind: "ENOENT"))?
	return ok(self.rows.map(fn row: row[index].clone()))

// Every row as a map from column name to value, like pgx's `RowToMap`.
fn Rows.maps() List[Map[String, Value]]:
	var out = List[Map[String, Value]]()

	for row in self.rows:
		var entry = Map[String, Value]()
		for i in 0..self.columns.length(): entry.add(
			self.columns[i].clone(),
			row[i].clone(),
		)
		out.append(entry)

	return out

// The only row, like pgx's `QueryRow`: an error with kind `ENOROWS` when there is none
// and `ETOOMANY` when there are several.
fn Rows.one() List[Value]!:
	if self.rows.is_empty(): return error(
		Error("postgres: no rows in result set", kind: "ENOROWS")
	)
	if self.rows.length() > 1: return error(
		Error("postgres: more than one row", kind: "ETOOMANY")
	)

	return ok(self.rows[0].clone())

// Learns types the server defines by name, so their columns decode: enums (as `Text`),
// composites (`Value.Composite`), domains (as their base type), ranges and multiranges
// of any subtype, and `hstore`. Each name also registers its array type. List a
// composite's field types too, or its fields stay text. Names are looked up as
// `regtype` text, so schema-qualified names and quotes work.
fn var Conn.load_types(names: List[String]) Void!:
	sql := "select t.oid::int8, t.typtype::text, t.typbasetype::int8, t.typarray::int8, coalesce(r.rngsubtype, 0)::int8, coalesce(r.rngmultitypid, 0)::int8, coalesce((select array_agg(a.atttypid::int8 order by a.attnum) from pg_attribute a where a.attrelid = t.typrelid and a.attnum > 0 and not a.attisdropped), '{}'), t.typname from pg_type t left join pg_range r on r.rngtypid = t.oid where t.oid in (select to_regtype(n)::oid from unnest($1::text[]) n)"
	rows := self.query(sql, [Value.Array(names.map(fn n: Value.Text(n.clone())))])?

	for row in rows.rows:
		oid := row[0].int().get(0)
		kind := row[1].text().get("")
		base := row[2].int().get(0)
		array_oid := row[3].int().get(0)
		subtype := row[4].int().get(0)
		multi := row[5].int().get(0)
		name := row[7].text().get("")
		var fields = List[Int]()
		for field in row[6].array().get([]): fields.append(field.int().get(0))

		if kind == "e": self.types.add(oid, TypeInfo("enum"))
		else if kind == "c": self.types.add(oid, TypeInfo("composite", 0, fields))
		else if kind == "d": self.types.add(oid, TypeInfo("domain", base))
		else if kind == "r": self.types.add(oid, TypeInfo("range", subtype))
		else if name == "hstore": self.types.add(oid, TypeInfo("hstore"))

		if multi > 0: self.types.add(multi, TypeInfo("multirange", subtype))
		if array_oid > 0 and kind != "b" or array_oid > 0 and name == "hstore":
			self.types.add(array_oid, TypeInfo("array", oid))

	return ok()

// Runs a statement with parameters over the simple protocol by substituting the values
// into the text on the client (pgx's simple protocol mode), for poolers such as
// PgBouncer in statement mode. Every result column is text-decoded.
fn var Conn.query_simple(sql: String, params: List[Value]) Rows!:
	return self.simple(interpolate(sql, params)?)

// Runs a statement whose placeholders are `@name`, with values from `args`.
fn var Conn.query_named(sql: String, args: Map[String, Value]) Rows!:
	rewritten := named(sql, args)?
	return self.query(rewritten.sql, rewritten.params)

// The command's first word: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `COPY` and so on.
fn Rows.verb() String:
	return self.command.split(" ", none)[0].clone()

// Frees every prepared statement on the server and forgets the cached ones.
fn var Conn.deallocate_all() Void!:
	void self.simple("DEALLOCATE ALL")?
	self.statements.clear()
	return ok()
