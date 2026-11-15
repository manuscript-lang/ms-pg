// Rows streamed from the server as it sends them. The query runs in its own task on its
// own connection, because a task cannot borrow a connection from its caller; rows cross
// to the caller over a pipe, so a slow reader slows the server (back pressure) and a
// large result never sits in memory whole.
import (
	Placement, Promise, PromiseOptions, promise from "@std/concurrency/promise.ms",
	ReadStream, Source, WriteStream, pipe from "@std/io/io.ms",

	Config, open from "@pg/conn.ms",
	Value from "@pg/value.ms",
)

// What the task runs. A promise keeps what it captures borrowed unless it is a stream,
// so the request travels over a pipe (issue #5).
type Request:
	config: Config
	sql: String
	params: List[Value]

fn send_row(var rows: WriteStream[List[Value]], row: List[Value]) Void!:
	return rows.write(row)

// Opens a connection, runs the statement and writes each row; the pipe closes at the
// end or on failure, and the failure is the task's result.
fn produce(var requests: ReadStream[Request], var rows: WriteStream[List[Value]]) Void!:
	next := requests.next()?
	if next.is_none(): return rows.close()

	request := next.get(Request(Config(), "", []))
	opened := open(request.config)

	if opened.is_error():
		void rows.close()
		return error(opened.err().clone())

	var conn = opened?
	result := conn.run_streaming(
		request.sql,
		request.params,
		fn row: send_row(var rows, row)
	)
	void conn.close()
	void rows.close()
	if result.is_error(): return error(result.err().clone())

	return ok()

type RowSource:
	rows: ReadStream[List[Value]]
	task: Promise[Void]
	finished: Bool

// The next row; at the end it waits for the task so that its failure (a server error,
// a login failure) is the stream's last item instead of a silent short result.
fn var RowSource.next() List[Value]?!:
	item := self.rows.next()?
	if item.is_some(): return ok(item)

	if not self.finished:
		self.finished = true
		self.task.wait()?

	return ok(none)

// Closing early closes the pipe, which stops the task's writes; its resulting error is
// expected and dropped.
fn var RowSource.close() Void!:
	void self.rows.close()

	if not self.finished:
		self.finished = true
		void self.task.wait()

	return ok()

// Runs `sql` on a new connection and streams its rows:
// `stream(config, sql) | filter(...)
// | map(...)`. `capacity` is how many rows may wait unread before the server is held
// back. Errors, including login and server errors, arrive as the stream's final item.
export fn stream(
	config: Config,
	sql: String,
	params: List[Value] = [],
	capacity: Int = 256,
) ReadStream[List[Value]]:
	var rows = pipe[List[Value]](capacity: capacity)
	var writer = rows.writer()
	var asks = pipe[Request](capacity: 1)
	var ask_writer = asks.writer()
	var ask_reader = asks.reader()
	void ask_writer.write(Request(config.clone(), sql.clone(), params.clone()))
	void ask_writer.close()

	task := promise(
		fn: produce(var ask_reader, var writer),
		options: PromiseOptions(placement: Placement.Spread)
	)
	return ReadStream[List[Value]](RowSource(rows.reader(), task, false))
