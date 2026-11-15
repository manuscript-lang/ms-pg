// The server's side of the protocol, for test servers, proxies and poolers: decode what
// a client sends and encode what a server replies.
import (
	Bytes, BytesBuilder from "@std/bytes.ms",

	Field, cstring, framed, i32, u16, u32 from "@pg/wire.ms",
)

// One message a client sends.
export enum Frontend:
	Query(sql: String)
	Parse(name: String, sql: String, oids: List[Int])
	Bind(
		portal: String,
		statement: String,
		params: List[Bytes?],
		param_formats: List[Int],
		result_formats: List[Int],
	)
	Describe(kind: UInt8, name: String)
	Execute(portal: String, max_rows: Int)
	Close(kind: UInt8, name: String)
	Sync
	Flush
	Terminate
	// A password, SASL or GSSAPI response: the raw body.
	Password(data: Bytes)
	CopyData(data: Bytes)
	CopyDone
	CopyFail(reason: String)

fn bad(what: String) Error:
	return Error(f"postgres: {what}", kind: "EPROTO")

type Text:
	value: String
	next: Int

fn read_text(data: Bytes, at: Int) Text!:
	var end = at
	while end < data.length() and data.get(end) != 0: end += 1
	if end >= data.length(): return error(bad("unterminated string"))

	piece := data.slice(at, end)?
	value := piece.decode_utf8().ok_or(bad("string is not UTF-8"))?
	return ok(Text(value, end + 1))

fn codes(data: Bytes, at: Int) List[Int]!:
	count := u16(data, at)?
	var out = List[Int]()
	for n in 0..count: out.append(u16(data, at + 2 + 2 * n)?)
	return ok(out)

// Decodes the body of a client message with type byte `code`.
export fn decode_frontend(code: UInt8, data: Bytes) Frontend!:
	if code == 81:
		sql := read_text(data, 0)?
		return ok(Frontend.Query(sql.value.clone()))

	if code == 80:
		name := read_text(data, 0)?
		sql := read_text(data, name.next)?
		count := u16(data, sql.next)?
		var found = List[Int]()
		for n in 0..count: found.append(u32(data, sql.next + 2 + 4 * n)?)
		return ok(Frontend.Parse(name.value.clone(), sql.value.clone(), found))

	if code == 66:
		portal := read_text(data, 0)?
		statement := read_text(data, portal.next)?
		formats := codes(data, statement.next)?
		var at = statement.next + 2 + 2 * formats.length()
		count := u16(data, at)?
		at += 2

		var params = List[Bytes?]()

		for _n in 0..count:
			size := i32(data, at)?
			at += 4

			if size < 0:
				params.append(none)
			else:
				params.append(data.slice(at, at + size)?)
				at += size

		results := codes(data, at)?
		return ok(
			Frontend.Bind(
				portal.value.clone(),
				statement.value.clone(),
				params,
				formats,
				results,
			)
		)

	if code == 68 or code == 67:
		name := read_text(data, 1)?
		if code == 68: return ok(Frontend.Describe(data.get(0), name.value.clone()))

		return ok(Frontend.Close(data.get(0), name.value.clone()))

	if code == 69:
		portal := read_text(data, 0)?
		return ok(Frontend.Execute(portal.value.clone(), i32(data, portal.next)?))

	if code == 83: return ok(Frontend.Sync)
	if code == 72: return ok(Frontend.Flush)
	if code == 88: return ok(Frontend.Terminate)
	if code == 112: return ok(Frontend.Password(data.clone()))
	if code == 100: return ok(Frontend.CopyData(data.clone()))
	if code == 99: return ok(Frontend.CopyDone)

	if code == 102:
		reason := read_text(data, 0)?
		return ok(Frontend.CopyFail(reason.value.clone()))

	return error(bad(f"unexpected client message type {code}"))

// The startup packet's parameters, from its body after the length (the protocol version
// first); an SSLRequest or CancelRequest has its own code and is told apart by it.
export fn decode_startup(data: Bytes) Map[String, String]!:
	var out = Map[String, String]()
	var at = 4

	while at < data.length() and data.get(at) != 0:
		key := read_text(data, at)?
		value := read_text(data, key.next)?
		out.add(key.value.clone(), value.value.clone())
		at = value.next

	return ok(out)

fn auth(kind: Int, rest: Bytes) Bytes!:
	var body = BytesBuilder()
	body.append_int32_be(kind.to_int32().get(0))
	body.append_bytes(rest)
	return framed(82, body.build())

export fn auth_ok() Bytes!:
	return auth(0, Bytes([]))

export fn auth_cleartext() Bytes!:
	return auth(3, Bytes([]))

export fn auth_md5(salt: Bytes) Bytes!:
	return auth(5, salt)

export fn auth_gss() Bytes!:
	return auth(7, Bytes([]))

export fn auth_gss_continue(token: Bytes) Bytes!:
	return auth(8, token)

// The SASL mechanisms on offer, then the server's challenge and final message.
export fn auth_sasl(mechanisms: List[String]) Bytes!:
	var body = BytesBuilder()
	for name in mechanisms: cstring(var body, name)
	body.append(0)
	return auth(10, body.build())

export fn auth_sasl_continue(data: Bytes) Bytes!:
	return auth(11, data)

export fn auth_sasl_final(data: Bytes) Bytes!:
	return auth(12, data)

export fn parameter_status(name: String, value: String) Bytes!:
	var body = BytesBuilder()
	cstring(var body, name)
	cstring(var body, value)
	return framed(83, body.build())

export fn backend_key_data(pid: Int, secret: Int) Bytes!:
	var body = BytesBuilder()
	body.append_int32_be(pid.to_int32().get(0))
	body.append_int32_be(secret.to_int32().get(0))
	return framed(75, body.build())

export fn ready_for_query(status: UInt8) Bytes!:
	var body = BytesBuilder()
	body.append(status)
	return framed(90, body.build())

export fn row_description(fields: List[Field]) Bytes!:
	var body = BytesBuilder()
	body.append_int16_be(fields.length().to_int16().get(0))

	for field in fields:
		cstring(var body, field.name)
		body.append_int32_be(0)
		body.append_int16_be(0)
		body.append_int32_be(field.type_oid.to_int32().get(0))
		body.append_int16_be(-1)
		body.append_int32_be(-1)
		body.append_int16_be(field.format.to_int16().get(0))

	return framed(84, body.build())

export fn data_row(columns: List[Bytes?]) Bytes!:
	var body = BytesBuilder()
	body.append_int16_be(columns.length().to_int16().get(0))

	for column in columns:
		if value := column:
			body.append_int32_be(value.length().to_int32().get(0))
			body.append_bytes(value)
		else:
			body.append_int32_be(-1)

	return framed(68, body.build())

export fn command_complete(tag: String) Bytes!:
	var body = BytesBuilder()
	cstring(var body, tag)
	return framed(67, body.build())

fn fields_message(code: UInt8, fields: Map[String, String]) Bytes!:
	var body = BytesBuilder()

	for entry in fields:
		body.append(entry.key.to_bytes().get(0))
		cstring(var body, entry.value)

	body.append(0)
	return framed(code, body.build())

// An error with fields keyed by their one-letter codes: `S` severity, `C` SQLSTATE, `M`
// message.
export fn error_response(fields: Map[String, String]) Bytes!:
	return fields_message(69, fields)

export fn notice_response(fields: Map[String, String]) Bytes!:
	return fields_message(78, fields)

export fn notification(pid: Int, channel: String, payload: String) Bytes!:
	var body = BytesBuilder()
	body.append_int32_be(pid.to_int32().get(0))
	cstring(var body, channel)
	cstring(var body, payload)
	return framed(65, body.build())

export fn parameter_description(oids: List[Int]) Bytes!:
	var body = BytesBuilder()
	body.append_int16_be(oids.length().to_int16().get(0))
	for oid in oids: body.append_int32_be(oid.to_int32().get(0))
	return framed(116, body.build())

fn empty(code: UInt8) Bytes!:
	return framed(code, Bytes([]))

export fn parse_complete() Bytes!:
	return empty(49)

export fn bind_complete() Bytes!:
	return empty(50)

export fn close_complete() Bytes!:
	return empty(51)

export fn no_data() Bytes!:
	return empty(110)

export fn empty_query() Bytes!:
	return empty(73)

export fn copy_in_response() Bytes!:
	return copy_response(71)

export fn copy_out_response() Bytes!:
	return copy_response(72)

export fn copy_both_response() Bytes!:
	return copy_response(87)

fn copy_response(code: UInt8) Bytes!:
	var body = BytesBuilder()
	body.append(0)
	body.append_int16_be(0)
	return framed(code, body.build())
