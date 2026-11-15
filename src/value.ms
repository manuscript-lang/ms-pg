// Values exchanged with the server. Parameters go out in text format; results come back
// in text or, for the common types, in binary format.
import (
	Bytes from "@std/bytes.ms",
	hex_decode, hex_encode from "@std/data/encoding.ms",
	Timestamp, parse_timestamp from "@std/time.ms",
)

// Type OIDs the client decodes from the binary format.
BOOL: Int := 16
BYTEA: Int := 17
INT8: Int := 20
INT2: Int := 21
INT4: Int := 23
TEXT: Int := 25
OID: Int := 26
FLOAT4: Int := 700
FLOAT8: Int := 701
VARCHAR: Int := 1043
DATE: Int := 1082
TIMESTAMP: Int := 1114
TIMESTAMPTZ: Int := 1184
UUID: Int := 2950
// Seconds from 1970-01-01 to 2000-01-01, PostgreSQL's epoch.
POSTGRES_EPOCH: Int := 946684800

// A query parameter or a result column.
export enum Value:
	Null
	Bool(value: Bool)
	Int(value: Int)
	Float(value: Float)
	Text(value: String)
	Bytes(value: Bytes)
	// A `timestamp` or `timestamptz`, as UTC. Only the binary format produces it; the
	// text format returns `Text`, since its meaning depends on the session time zone.
	Timestamp(value: Timestamp)
	// A `date`, as days since 1970-01-01. Only the binary format produces it.
	Date(days: Int)

fn Value.as_text() String?:
	match self:
		Value.Null: return none
		Value.Bool(value): return if value: "t" else: "f"
		Value.Int(value): return f"{value}"
		Value.Float(value): return f"{value}"
		Value.Text(value): return value.clone()
		Value.Bytes(value): return f"\\x{hex_encode(value)}"
		Value.Timestamp(value): return value.to_string()
		Value.Date(days):
			day := Timestamp(seconds: days * 86400, nanos: 0).to_string()
			return day.slice(0, 10).get("")

// Typed access: each returns the payload when the value is that variant, and none for
// any other variant (including `Null`).
fn Value.int() Int?:
	match self:
		Value.Int(value): return value
		_: return none

fn Value.float() Float?:
	match self:
		Value.Float(value): return value
		_: return none

fn Value.bool() Bool?:
	match self:
		Value.Bool(value): return value
		_: return none

fn Value.text() String?:
	match self:
		Value.Text(value): return value.clone()
		_: return none

fn Value.bytes() Bytes?:
	match self:
		Value.Bytes(value): return value.clone()
		_: return none

fn Value.timestamp() Timestamp?:
	match self:
		Value.Timestamp(value): return value
		_: return none

fn Value.date() Int?:
	match self:
		Value.Date(days): return days
		_: return none

fn Value.is_null() Bool:
	match self:
		Value.Null: return true
		_: return false

// The text-format bytes sent for a parameter; none is NULL.
export fn encode_value(value: Value) Bytes?:
	text := value.as_text()
	if text.is_none(): return none

	return text.get("").to_bytes()

fn bad(type_name: String, text: String) Error:
	return Error(
		f"postgres: invalid {type_name} value from server: {text}",
		kind: "EBADMSG",
	)

// Normalizes PostgreSQL's timestamp text ("2026-01-01 00:00:01.5+00", "... +05:30" or
// no offset) to RFC 3339. A `timestamp` has no offset and is read as UTC, as in binary.
fn rfc3339(text: String) String:
	joined := text.replace(" ", "T")
	if joined.length() < 11: return joined

	sign := joined.slice(joined.length() - 3, joined.length() - 2).get("")
	if sign == "+" or sign == "-": return f"{joined}:00"

	longer := joined.slice(joined.length() - 6, joined.length() - 5).get("")
	if longer == "+" or longer == "-": return joined
	if joined.ends_with("Z"): return joined

	return f"{joined}Z"

fn decode_text_timestamp(text: String) Value!:
	if text == "infinity" or text == "-infinity": return ok(Value.Text(text))

	stamp := parse_timestamp(rfc3339(text)).map_error(fn _e: bad("timestamp", text))?
	return ok(Value.Timestamp(stamp))

fn decode_text_date(text: String) Value!:
	if text == "infinity" or text == "-infinity": return ok(Value.Text(text))

	stamp := parse_timestamp(f"{text}T00:00:00Z").map_error(fn _e: bad("date", text))?
	return ok(Value.Date(stamp.seconds / 86400))

// Decodes a column sent in text format into the same `Value` the binary format gives.
fn decode_text(type_oid: Int, text: String) Value!:
	if type_oid == BOOL: return ok(Value.Bool(text == "t"))
	if type_oid == INT8 or type_oid == INT2 or type_oid == INT4 or type_oid == OID:
		return ok(Value.Int(text.parse_int().ok_or(bad("integer", text))?))
	if type_oid == FLOAT8:
		return ok(Value.Float(text.parse_float().ok_or(bad("float", text))?))

	if type_oid == FLOAT4:
		// Text holds the shortest decimal of a float4 ("0.1"). Round it to float4 and
		// widen it, which is the value the binary format carries (0.10000000149011612).
		wide := text.parse_float().ok_or(bad("float", text))?
		single := wide.to_float32().ok_or(bad("float4", text))?
		return ok(Value.Float(single.to_float()))

	if type_oid == TIMESTAMP or type_oid == TIMESTAMPTZ:
		return decode_text_timestamp(text)
	if type_oid == DATE: return decode_text_date(text)

	if type_oid == BYTEA:
		if not text.starts_with("\\x"): return error(bad("bytea", text))
		return ok(Value.Bytes(hex_decode(text.slice(2, text.length())?, none)?))

	return ok(Value.Text(text))

// Decodes one column by the column's type OID and format code (0 text, 1 binary). Both
// formats give the same `Value` variant for a type; a value that does not parse is an
// error.
export fn decode_value(type_oid: Int, format: Int, raw: Bytes?) Value!:
	if raw.is_none(): return ok(Value.Null)

	bytes := raw.get(Bytes([]))
	if format == 1: return decode_binary(type_oid, bytes)

	text := bytes.decode_utf8().ok_or(
		Error("postgres: column is not valid UTF-8", kind: "EBADMSG")
	)?
	return decode_text(type_oid, text)

// Whether `type_oid` has a binary decoder here; other columns stay in text format.
export fn is_binary_type(type_oid: Int) Bool:
	return (
		type_oid == BOOL
		or type_oid == BYTEA
		or type_oid == INT8
		or type_oid == INT2
		or type_oid == INT4
		or type_oid == TEXT
		or type_oid == OID
		or type_oid == FLOAT4
		or type_oid == FLOAT8
		or type_oid == VARCHAR
		or type_oid == DATE
		or type_oid == TIMESTAMP
		or type_oid == TIMESTAMPTZ
		or type_oid == UUID
	)

fn size_error(type_name: String, size: Int) Error:
	return Error(
		f"postgres: invalid binary {type_name} of {size} bytes from server",
		kind: "EBADMSG",
	)

fn expect_size(type_name: String, bytes: Bytes, size: Int) Void!:
	if bytes.length() != size: return error(size_error(type_name, bytes.length()))
	return ok()

// Microseconds since 2000-01-01 as a timestamp. The infinities PostgreSQL stores as the
// extreme integers read as the text the server sends for them.
fn decode_timestamp(micros: Int) Value:
	if micros == 9223372036854775807: return Value.Text("infinity")
	if micros == -9223372036854775807 - 1: return Value.Text("-infinity")

	var fraction = micros % 1000000
	if fraction < 0: fraction += 1000000

	seconds := (micros - fraction) / 1000000 + POSTGRES_EPOCH
	return Value.Timestamp(Timestamp(seconds: seconds, nanos: fraction * 1000))

fn hex_digits(bytes: Bytes, from: Int, to: Int) String!:
	return ok(hex_encode(bytes.slice(from, to)?))

fn decode_uuid(bytes: Bytes) Value!:
	a := hex_digits(bytes, 0, 4)?
	b := hex_digits(bytes, 4, 6)?
	c := hex_digits(bytes, 6, 8)?
	d := hex_digits(bytes, 8, 10)?
	e := hex_digits(bytes, 10, 16)?
	return ok(Value.Text(f"{a}-{b}-{c}-{d}-{e}"))

// Decodes one column from its binary form, by the column's type OID.
fn decode_binary(type_oid: Int, bytes: Bytes) Value!:
	if type_oid == BOOL:
		expect_size("bool", bytes, 1)?
		return ok(Value.Bool(bytes.get(0) != 0))

	if type_oid == INT2:
		expect_size("int2", bytes, 2)?
		return ok(Value.Int((bytes.int16_be(0)?).to_int()))

	if type_oid == INT4:
		expect_size("int4", bytes, 4)?
		return ok(Value.Int((bytes.int32_be(0)?).to_int()))

	if type_oid == OID:
		expect_size("oid", bytes, 4)?
		return ok(Value.Int((bytes.uint32_be(0)?).to_int()))

	if type_oid == INT8:
		expect_size("int8", bytes, 8)?
		return ok(Value.Int(bytes.int64_be(0)?))

	if type_oid == FLOAT4:
		expect_size("float4", bytes, 4)?
		return ok(Value.Float((bytes.float32_be(0)?).to_float()))

	if type_oid == FLOAT8:
		expect_size("float8", bytes, 8)?
		return ok(Value.Float(bytes.float64_be(0)?))

	if type_oid == TIMESTAMP or type_oid == TIMESTAMPTZ:
		expect_size("timestamp", bytes, 8)?
		return ok(decode_timestamp(bytes.int64_be(0)?))

	if type_oid == DATE:
		expect_size("date", bytes, 4)?

		days := (bytes.int32_be(0)?).to_int()
		if days == 2147483647: return ok(Value.Text("infinity"))
		if days == -2147483648: return ok(Value.Text("-infinity"))

		return ok(Value.Date(days + POSTGRES_EPOCH / 86400))

	if type_oid == UUID:
		expect_size("uuid", bytes, 16)?
		return decode_uuid(bytes)

	if type_oid == BYTEA: return ok(Value.Bytes(bytes.clone()))

	text := bytes.decode_utf8().ok_or(
		Error("postgres: column is not valid UTF-8", kind: "EBADMSG")
	)?
	return ok(Value.Text(text))
