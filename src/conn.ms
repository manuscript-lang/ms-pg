// One PostgreSQL connection: startup, authentication, queries and transactions.
import (
	Bytes, BytesBuilder from "@std/bytes.ms",
	ReadStream from "@std/io/io.ms",
	from_list from "@std/io/streams.ms",
	Connection, connect, connect_unix, start_tls from "@std/net/net.ms",
	TlsOptions from "@std/net/tls.ms",
	Duration from "@std/time.ms",

	md5_password, scram_start from "@pg/auth.ms",
	Value, decode_value, encode_value, is_binary_type from "@pg/value.ms",
	Backend,
	Field,
	PORTAL,
	STATEMENT,
	STATUS_IDLE,
	bind,
	cancel_request,
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
	// NOTIFY messages that arrived while a statement was running, oldest first.
	notifications: List[Notification]

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

// Connects, authenticates, and returns a connection ready for queries.
export fn open(config: Config) Conn!:
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
		[],
	)
	var parameters = config.params.clone()
	parameters.add("application_name", config.application_name.clone())
	conn.send(startup(config.user, config.database, parameters)?)?
	conn.authenticate(config)?
	return ok(conn)

fn var Conn.authenticate(config: Config) Void!:
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
	out.append_bytes(bind("", "", params.map(fn p: encode_value(p)), [])?)
	out.append_bytes(describe(PORTAL, "")?)
	out.append_bytes(execute("", 0)?)
	out.append_bytes(sync()?)
	self.send(out.build())?
	return self.pump([], emit)

// Runs one statement with parameters in a single round trip.
fn var Conn.query(sql: String, params: List[Value]) Rows!:
	var out = BytesBuilder()
	out.append_bytes(parse("", sql, [])?)
	out.append_bytes(bind("", "", params.map(fn p: encode_value(p)), [])?)
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

// Parses `sql` under `name` and learns its result columns.
fn var Conn.prepare(name: String, sql: String) Prepared!:
	var out = BytesBuilder()
	out.append_bytes(parse(name, sql, [])?)
	out.append_bytes(describe(STATEMENT, name)?)
	out.append_bytes(sync()?)
	self.send(out.build())?

	var columns = List[Field]()
	var failure: Error? = none

	while true:
		message := self.read()?

		match message:
			Backend.RowDescription(found): columns = found.clone()
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
				if is_binary_type(c.type_oid): 1 else: 0,
			)
		)
	return ok(Prepared(name.clone(), columns))

// Runs a prepared statement: bind, execute and sync in one round trip.
fn var Conn.run(statement: Prepared, params: List[Value]) Rows!:
	var out = BytesBuilder()
	formats := statement.columns.map(fn c: c.format)
	out.append_bytes(
		bind("", statement.name, params.map(fn p: encode_value(p)), formats)?
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
	var out = BytesBuilder()

	for i in 0..batch.length():
		out.append_bytes(parse("", batch.sql[i], [])?)
		out.append_bytes(bind("", "", batch.params[i].map(fn p: encode_value(p)), [])?)
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

// Loads rows into a table with `COPY ... FROM STDIN`, in the text format, and returns
// how many the server stored. `table` is the table's name parts
// (`["public", "items"]`); the names are quoted. Rows come from a stream, so a pipeline
// feeds it:
// `conn.copy_from(["items"], ["id", "name"], rows | map(...))`. Rows are sent in
// batches of at most 64 KiB. When the stream fails, the COPY is aborted and nothing is
// stored.
fn var Conn.copy_from(
	table: List[String],
	columns: List[String],
	rows: ReadStream[List[Value]],
) Int!:
	target := table.map(fn t: quote_ident(t)).join(".")
	names := if columns.is_empty(): "" else: f" ({columns.map(fn c: quote_ident(c)).join(", ")})"
	self.send(query(f"COPY {target}{names} FROM STDIN")?)?

	var failure: Error? = none

	while true:
		message := self.read()?

		match message:
			Backend.CopyInResponse: break
			Backend.ErrorResponse(found):
				failure = server_error(found)
			Backend.ReadyForQuery(status):
				self.status = status
				if f := failure: return error(f)

				return error(protocol("COPY did not start"))
			_: continue

	var pending = BytesBuilder()
	var size = 0
	var source = rows
	var cause: Error? = none

	while true:
		next := source.next()

		if next.is_error():
			cause = next.err().clone()
			break

		item := next?
		if item.is_none(): break

		line := copy_line(item.get([]))
		size += line.length()
		pending.append_bytes(line)

		if size >= 65536:
			self.send(copy_data(pending.drain())?)?
			size = 0

	if cause.is_some():
		self.send(copy_fail("input failed")?)?
	else:
		if size > 0: self.send(copy_data(pending.drain())?)?
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
