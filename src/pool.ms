// A connection pool: a fixed set of worker tasks, each owning one connection, fed over
// pipes. A connection cannot move between tasks, so queries travel to the workers and
// rows travel back.
import (
	Placement, Promise, PromiseOptions, promise from "@std/concurrency/promise.ms",
	Pipe, ReadStream, WriteStream, pipe from "@std/io/io.ms",

	Config, Conn, Rows, open from "@pg/conn.ms",
	Value from "@pg/value.ms",
)

// One statement and its parameters.
export type Statement:
	sql: String
	params: List[Value] = []
	// Send each row back as it arrives, then one closing reply (`Pool.each`).
	stream: Bool = false

// One message from a worker: a whole result, a failure, or (streaming) one row. A
// streamed statement ends with a reply that has no `row`: its `rows` hold the column
// names and command tag only, or `failure` is set.
export type Reply:
	rows: Rows
	failure: Error?
	row: List[Value]? = none

fn failed(cause: Error) Reply:
	return Reply(Rows([], [], ""), cause)

fn send_row(var replies: WriteStream[Reply], row: List[Value]) Void!:
	return replies.write(Reply(Rows([], [], ""), none, row))

// Runs one streamed statement, writing a reply per row and a closing reply.
fn serve_streaming(var conn: Conn, request: Statement, var replies: WriteStream[Reply]) Void!:
	result := conn.run_streaming(
		request.sql,
		request.params,
		fn row: send_row(var replies, row)
	)
	if result.is_error(): return replies.write(failed(result.err().clone()))

	return replies.write(Reply(result?, none))

// Answers every request with the connect failure.
fn drain(
	cause: Error,
	var requests: ReadStream[Statement],
	var replies: WriteStream[Reply],
) Void!:
	for _request in requests: replies.write(failed(cause.clone()))?
	return replies.close()

// Runs requests on its own connection until the request stream ends.
fn serve(
	var conn: Conn,
	var requests: ReadStream[Statement],
	var replies: WriteStream[Reply],
) Void!:
	for request in requests:
		if request.stream:
			serve_streaming(var conn, request, var replies)?
			continue

		result := conn.query(request.sql, request.params)

		if result.is_error():
			replies.write(failed(result.err().clone()))?
		else:
			replies.write(Reply(result?, none))?

	conn.close()?
	return replies.close()

// The first and only value on `settings` is the connection config: a promise keeps what
// it captures borrowed unless it is a stream, so the config travels over a pipe too.
fn worker(
	var settings: ReadStream[Config],
	var requests: ReadStream[Statement],
	var replies: WriteStream[Reply],
) Void!:
	config := settings.next()?
	if config.is_none(): return replies.close()

	opened := open(config.get(Config()))
	if opened.is_error(): return drain(opened.err().clone(), var requests, var replies)

	var conn = opened?
	return serve(var conn, var requests, var replies)

export type Slot:
	requests: WriteStream[Statement]
	replies: ReadStream[Reply]
	task: Promise[Void]

// A fixed-size pool; owned by one task. Statements go to workers in turn.
export opaque type Pool:
	slots: List[Slot]
	next: Int

// Starts `size` workers, each opening its own connection.
export fn start(config: Config, size: Int) Pool!:
	if size < 1: return error(
		Error("postgres: a pool needs at least one worker", kind: "EINVAL")
	)

	var slots = List[Slot]()

	for _i in 0..size:
		var requests = pipe[Statement](capacity: 64)
		var replies = pipe[Reply](capacity: 64)
		var request_writer = requests.writer()
		var reply_writer = replies.writer()
		var request_reader = requests.reader()
		var reply_reader = replies.reader()
		var settings = pipe[Config](capacity: 1)
		var settings_writer = settings.writer()
		var settings_reader = settings.reader()
		settings_writer.write(config.clone())?
		settings_writer.close()?

		task := promise(
			fn: worker(
				var settings_reader,
				var request_reader,
				var reply_writer,
			),
			options: PromiseOptions(placement: Placement.Spread)
		)
		slots.append(Slot(request_writer, reply_reader, task))

	return ok(Pool(slots, 0))

// Queues a statement on the next worker.
fn var Pool.submit(statement: Statement) Int!:
	index := self.next % self.slots.length()
	self.next += 1
	self.slots[index].requests.write(statement.clone())?
	return ok(index)

fn var Pool.reply(index: Int) Reply!:
	reply := self.slots[index].replies.next()?
	if reply.is_none(): return error(
		Error("postgres: pool worker stopped", kind: "ECONNRESET")
	)

	return ok(reply.get(failed(Error("postgres: no reply"))))

fn var Pool.take(index: Int) Rows!:
	got := self.reply(index)?
	if f := got.failure: return error(f.clone())

	return ok(got.rows.clone())

// Runs one statement on a pooled connection and hands each row to `each` as the server
// sends it, so a large result never sits in memory. The result holds the column names
// and command tag, with no rows. If `each` fails, the rest of the result is read and
// dropped so the worker stays in step, and the first error is returned.
fn var Pool.each(sql: String, params: List[Value], each: fn(List[Value]) Void!) Rows!:
	index := self.submit(Statement(sql.clone(), params.clone(), true))?
	var failure: Error? = none

	while true:
		got := self.reply(index)?

		if row := got.row:
			if failure.is_none():
				handled := each(row)
				if handled.is_error(): failure = handled.err().clone()

			continue

		if f := failure: return error(f)
		if f := got.failure: return error(f.clone())

		return ok(got.rows.clone())

	return error(Error("postgres: unreachable", kind: "EPROTO"))

// Runs one statement on a pooled connection.
fn var Pool.query(sql: String, params: List[Value] = []) Rows!:
	index := self.submit(Statement(sql.clone(), params.clone()))?
	return self.take(index)

// Statements queued per worker before their replies are read. A worker blocks writing
// replies once its reply pipe is full, so queueing more than the pipe capacity (64)
// before reading would deadlock.
BATCH_PER_WORKER: Int := 32

// Runs statements concurrently across the workers; results are in statement order. The
// first failure is returned after every reply has been read.
fn var Pool.query_all(statements: List[Statement]) List[Rows]!:
	var results = List[Rows]()
	var failure: Error? = none
	window := self.slots.length() * BATCH_PER_WORKER
	var start = 0

	while start < statements.length():
		var order = List[Int]()
		stop := (start + window).min(statements.length())
		for i in start..stop: order.append(self.submit(statements[i].clone())?)

		for index in order:
			got := self.take(index)

			if got.is_error():
				if failure.is_none(): failure = got.err().clone()
			else:
				results.append(got?)

		start = stop

	if f := failure: return error(f)

	return ok(results)

// Ends the workers and closes their connections. Closing twice is harmless.
fn var Pool.close() Void!:
	for i in 0..self.slots.length():
		self.slots[i].requests.close()?
	for i in 0..self.slots.length():
		self.slots[i].task.wait()?
	self.slots = []
	return ok()
