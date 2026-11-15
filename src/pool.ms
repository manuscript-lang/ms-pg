// A connection pool: a fixed set of worker tasks, each owning one connection, fed over
// pipes. A connection cannot move between tasks, so queries travel to the workers and
// rows travel back.
import (
	Placement, Promise, PromiseOptions, promise from "@std/concurrency/promise.ms",
	Pipe, ReadStream, WriteStream, pipe from "@std/io/io.ms",
	Deadline, Duration, deadline_after from "@std/time.ms",

	Batch, Config, Conn, Rows, open from "@pg/conn.ms",
	Value from "@pg/value.ms",
)

// One statement and its parameters.
export type Statement:
	sql: String
	params: List[Value] = []

// How a worker runs a job's statements: one statement, all of them in one transaction,
// or all of them pipelined in one round trip.
export enum Mode:
	Single
	Transaction
	Pipelined

// The work sent to a worker.
export type Job:
	statements: List[Statement]
	mode: Mode

export type Reply:
	results: List[Rows]
	failure: Error?

fn failed(cause: Error) Reply:
	return Reply([], cause)

// What a worker needs: the connection settings and the limits that retire a connection.
type Settings:
	config: Config
	max_lifetime: Duration?
	max_idle: Duration?

// Limits on a pooled connection, as pgx's `MaxConnLifetime` and `MaxConnIdleTime`. A
// worker replaces a connection that is older or idler than the limit before its next
// job, and one that failed with a connection error (as opposed to a server error).
export type PoolOptions:
	max_lifetime: Duration? = none
	max_idle: Duration? = none

// Answers every request with the connect failure.
fn drain(cause: Error, var jobs: ReadStream[Job], var replies: WriteStream[Reply]) Void!:
	for _job in jobs: replies.write(failed(cause.clone()))?
	return replies.close()

// A server error carries a five character SQLSTATE, and most leave the connection
// usable. A connection exception (class 08), a server shutdown (57P) or any other kind
// (a reset, a protocol error, a timeout) means the connection is no longer trustworthy.
fn connection_failed(cause: Error) Bool:
	kind := cause.kind.get("")
	return kind.length() != 5 or kind.starts_with("08") or kind.starts_with("57P")

fn run_job(var conn: Conn, job: Job) List[Rows]!:
	if job.mode == Mode.Pipelined:
		var batch = Batch(sql: [], params: [])
		for statement in job.statements: batch.add(statement.sql, statement.params)
		return conn.send_batch(batch)

	if job.mode == Mode.Transaction:
		return conn.transaction(
			fn(var c: Conn):
				var results = List[Rows]()
				for statement in job.statements: results.append(
					c.query(statement.sql, statement.params)?
				)
				return ok(results)
		)

	var results = List[Rows]()
	for statement in job.statements: results.append(
		conn.query(statement.sql, statement.params)?
	)
	return ok(results)

// Runs jobs on its own connection, opening a new one when the old one is retired. A
// connection that could not be replaced stays marked stale and the next job tries
// again.
fn serve(
	settings: Settings,
	first: Conn,
	var jobs: ReadStream[Job],
	var replies: WriteStream[Reply],
) Void!:
	var conn = first
	var stale = false
	var born = lifetime_deadline(settings.max_lifetime.clone())
	var idle = lifetime_deadline(settings.max_idle.clone())

	for job in jobs:
		if stale or born.expired() or idle.expired():
			void conn.close()

			opened := open(settings.config.clone())

			if opened.is_error():
				stale = true
				replies.write(failed(opened.err().clone()))?
				continue

			conn = opened?
			stale = false
			born = lifetime_deadline(settings.max_lifetime.clone())

		result := run_job(var conn, job)
		idle = lifetime_deadline(settings.max_idle.clone())

		if result.is_error():
			if connection_failed(result.err()): stale = true
			replies.write(failed(result.err().clone()))?
		else:
			replies.write(Reply(result?, none))?

	void conn.close()
	return replies.close()

// A deadline far away when there is no limit.
fn lifetime_deadline(limit: Duration?) Deadline:
	return deadline_after(limit.get(Duration(nanos: 9000000000000000000)))

// The first and only value on `settings` is what the worker needs: a promise keeps what
// it captures borrowed unless it is a stream, so the settings travel over a pipe too.
fn worker(
	var settings: ReadStream[Settings],
	var jobs: ReadStream[Job],
	var replies: WriteStream[Reply],
) Void!:
	received := settings.next()?
	if received.is_none(): return replies.close()

	given := received.get(Settings(Config(), none, none))
	opened := open(given.config.clone())
	if opened.is_error(): return drain(opened.err().clone(), var jobs, var replies)

	conn := opened?
	return serve(given, conn, var jobs, var replies)

export type Slot:
	requests: WriteStream[Job]
	replies: ReadStream[Reply]
	task: Promise[Void]

// A fixed-size pool; owned by one task. Jobs go to workers in turn.
export opaque type Pool:
	slots: List[Slot]
	next: Int

// Starts `size` workers, each opening its own connection.
export fn start(config: Config, size: Int) Pool!:
	return start_with(config, size, PoolOptions())

// Like `start`, with connection limits.
export fn start_with(config: Config, size: Int, options: PoolOptions) Pool!:
	if size < 1: return error(
		Error("postgres: a pool needs at least one worker", kind: "EINVAL")
	)

	var slots = List[Slot]()

	for _i in 0..size:
		var requests = pipe[Job](capacity: 64)
		var replies = pipe[Reply](capacity: 64)
		var request_writer = requests.writer()
		var reply_writer = replies.writer()
		var request_reader = requests.reader()
		var reply_reader = replies.reader()
		var settings = pipe[Settings](capacity: 1)
		var settings_writer = settings.writer()
		var settings_reader = settings.reader()
		settings_writer.write(
			Settings(
				config.clone(),
				options.max_lifetime.clone(),
				options.max_idle.clone(),
			)
		)?
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

// Queues a job on the next worker.
fn var Pool.submit(job: Job) Int!:
	index := self.next % self.slots.length()
	self.next += 1
	self.slots[index].requests.write(job.clone())?
	return ok(index)

fn var Pool.take(index: Int) List[Rows]!:
	reply := self.slots[index].replies.next()?
	if reply.is_none(): return error(
		Error("postgres: pool worker stopped", kind: "ECONNRESET")
	)

	got := reply.get(failed(Error("postgres: no reply")))
	if f := got.failure: return error(f.clone())

	return ok(got.results.clone())

fn var Pool.run_one(job: Job) List[Rows]!:
	index := self.submit(job)?
	return self.take(index)

// Runs one statement on a pooled connection.
fn var Pool.query(sql: String, params: List[Value] = []) Rows!:
	results := self.run_one(Job([Statement(sql.clone(), params.clone())], Mode.Single))?
	return ok(results[0].clone())

// Runs the statements in order inside one transaction on one pooled connection, and
// commits when all succeed; a failure rolls everything back. This is the pool's
// counterpart of pgx's `Acquire` followed by `Begin`: a connection cannot leave its
// task, so the whole unit of work travels to it.
fn var Pool.transaction(statements: List[Statement]) List[Rows]!:
	return self.run_one(Job(statements.clone(), Mode.Transaction))

// Sends the statements pipelined in one round trip on one pooled connection (see
// `Conn.send_batch`).
fn var Pool.batch(statements: List[Statement]) List[Rows]!:
	return self.run_one(Job(statements.clone(), Mode.Pipelined))

// Checks that a pooled connection answers; a dead one is replaced on the next job.
fn var Pool.ping() Void!:
	void self.query("select 1")?
	return ok()

// How many workers, and so connections, the pool runs.
fn Pool.size() Int:
	return self.slots.length()

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
		for i in start..stop:
			order.append(self.submit(Job([statements[i].clone()], Mode.Single))?)

		for index in order:
			got := self.take(index)

			if got.is_error():
				if failure.is_none(): failure = got.err().clone()
			else:
				results.append(got? [0].clone())

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
