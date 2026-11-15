// Large objects: server-side files stored by OID, read and written in pieces. Every
// call must run inside a transaction, as the server requires.
import (
	Bytes from "@std/bytes.ms",

	Conn from "@pg/conn.ms",
	Value from "@pg/value.ms",
)

// How an object is opened.
export enum LoMode:
	Read
	Write
	ReadWrite

// Where a seek counts from.
export enum Whence:
	Start
	Current
	End

fn mode_bits(mode: LoMode) Int:
	match mode:
		LoMode.Read: return 262144
		LoMode.Write: return 131072
		LoMode.ReadWrite: return 393216

fn whence_code(whence: Whence) Int:
	match whence:
		Whence.Start: return 0
		Whence.Current: return 1
		Whence.End: return 2

fn call(var conn: Conn, sql: String, params: List[Value]) Value!:
	rows := conn.query(sql, params)?
	return ok(rows.one()? [0].clone())

fn number(value: Value) Int!:
	return value.int().ok_or(
		Error("postgres: unexpected large object reply", kind: "EPROTO")
	)

// Creates an object and returns its OID; pass 0 to let the server pick one.
export fn lo_create(var conn: Conn, oid: Int) Int!:
	return number(call(var conn, "select lo_create($1::oid)", [Value.Int(oid)])?)

// Opens an object and returns a descriptor valid until the transaction ends.
export fn lo_open(var conn: Conn, oid: Int, mode: LoMode) Int!:
	return number(
		call(
			var conn,
			"select lo_open($1::oid, $2::int4)",
			[Value.Int(oid), Value.Int(mode_bits(mode))],
		)?
	)

export fn lo_write(var conn: Conn, fd: Int, data: Bytes) Int!:
	return number(
		call(
			var conn,
			"select lowrite($1::int4, $2::bytea)",
			[Value.Int(fd), Value.Bytes(data.clone())],
		)?
	)

// Reads up to `count` bytes; fewer (or none) at the end of the object.
export fn lo_read(var conn: Conn, fd: Int, count: Int) Bytes!:
	value := call(
		var conn,
		"select loread($1::int4, $2::int4)",
		[Value.Int(fd), Value.Int(count)],
	)?
	return value.bytes().ok_or(
		Error("postgres: unexpected large object reply", kind: "EPROTO")
	)

// Moves the position and returns the new one.
export fn lo_seek(var conn: Conn, fd: Int, offset: Int, whence: Whence) Int!:
	return number(
		call(
			var conn,
			"select lo_lseek64($1::int4, $2::int8, $3::int4)",
			[
				Value.Int(fd),
				Value.Int(offset),
				Value.Int(whence_code(whence)),
			],
		)?
	)

export fn lo_tell(var conn: Conn, fd: Int) Int!:
	return number(call(var conn, "select lo_tell64($1::int4)", [Value.Int(fd)])?)

export fn lo_truncate(var conn: Conn, fd: Int, size: Int) Void!:
	void call(
		var conn,
		"select lo_truncate64($1::int4, $2::int8)",
		[Value.Int(fd), Value.Int(size)],
	)?
	return ok()

export fn lo_close(var conn: Conn, fd: Int) Void!:
	void call(var conn, "select lo_close($1::int4)", [Value.Int(fd)])?
	return ok()

// Deletes the object.
export fn lo_unlink(var conn: Conn, oid: Int) Void!:
	void call(var conn, "select lo_unlink($1::oid)", [Value.Int(oid)])?
	return ok()
