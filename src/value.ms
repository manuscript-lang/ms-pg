// Values exchanged with the server. Parameters go out in text format; results come back
// in text or, for the common types, in binary format.
import (
	Bytes, BytesBuilder from "@std/bytes.ms",
	hex_decode, hex_encode from "@std/data/encoding.ms",
	Decimal, parse_decimal from "@std/math/decimal.ms",
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
JSON: Int := 114
JSONB: Int := 3802
NUMERIC: Int := 1700
INTERVAL: Int := 1186
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
	// A `json` or `jsonb` document, as its text.
	Json(text: String)
	// A `numeric`. `NaN` and the infinities stay `Text`.
	Decimal(value: Decimal)
	// An `interval`: months and days stay apart from the microseconds, as in the
	// server.
	Interval(months: Int, days: Int, micros: Int)
	// An array column or parameter, with nested arrays for more dimensions.
	Array(items: List[Value])

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
		Value.Json(text): return text.clone()
		Value.Decimal(value): return value.to_string()
		Value.Interval(months, days, micros):
			return f"{months} mons {days} days {micros} microseconds"
		Value.Array(items): return array_literal(items)

// Quotes one array element: a backslash or double quote is escaped.
fn array_element(item: Value) String:
	match item:
		Value.Array(items): return array_literal(items)
		_:
			text := item.as_text()
			if text.is_none(): return "NULL"

			return f"\"{text.get("").replace("\\", "\\\\").replace("\"", "\\\"")}\""

// An array literal such as `{"a","b",NULL}`.
fn array_literal(items: List[Value]) String:
	joined := items.map(fn item: array_element(item)).join(",")
	return f"\{{joined}\}"

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

fn Value.json() String?:
	match self:
		Value.Json(text): return text.clone()
		_: return none

fn Value.decimal() Decimal?:
	match self:
		Value.Decimal(value): return value
		_: return none

fn Value.array() List[Value]?:
	match self:
		Value.Array(items): return items.clone()
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

// The element type of an array type, by OID; none for any other type.
fn element_of(type_oid: Int) Int?:
	if type_oid == 1000: return BOOL
	if type_oid == 1001: return BYTEA
	if type_oid == 1005: return INT2
	if type_oid == 1007: return INT4
	if type_oid == 1009: return TEXT
	if type_oid == 1015: return VARCHAR
	if type_oid == 1016: return INT8
	if type_oid == 1021: return FLOAT4
	if type_oid == 1022: return FLOAT8
	if type_oid == 1028: return OID
	if type_oid == 1115: return TIMESTAMP
	if type_oid == 1182: return DATE
	if type_oid == 1185: return TIMESTAMPTZ
	if type_oid == 1187: return INTERVAL
	if type_oid == 1231: return NUMERIC
	if type_oid == 2951: return UUID
	if type_oid == 199: return JSON
	if type_oid == 3807: return JSONB

	return none

type ArrayPart:
	items: List[Value]
	next: Int

// Reads the array whose `{` is at byte `at`, and returns the byte after its `}`.
fn parse_array(bytes: Bytes, at: Int, element_oid: Int) ArrayPart!:
	var items = List[Value]()
	var i = at + 1
	if bytes.get(i) == 125: return ok(ArrayPart(items, i + 1))

	while true:
		here := bytes.get(i)

		if here == 123:
			inner := parse_array(bytes, i, element_oid)?
			items.append(Value.Array(inner.items))
			i = inner.next
		else if here == 34:
			var raw = BytesBuilder()
			i += 1

			while bytes.get(i) != 34:
				if bytes.get(i) == 92: i += 1
				raw.append(bytes.get(i))
				i += 1

			i += 1

			text := raw.build().decode_utf8().ok_or(bad("array", "invalid UTF-8"))?
			items.append(decode_text(element_oid, text)?)
		else:
			start := i
			while bytes.get(i) != 44 and bytes.get(i) != 125: i += 1

			piece := bytes.slice(start, i)?
			text := piece.decode_utf8().ok_or(bad("array", "invalid UTF-8"))?

			if text.equal_fold("NULL"): items.append(Value.Null)
			else: items.append(decode_text(element_oid, text)?)

		if bytes.get(i) == 125: return ok(ArrayPart(items, i + 1))
		if bytes.get(i) != 44: return error(bad("array", "expected a comma"))

		i += 1

	return error(bad("array", "unterminated"))

fn decode_text_array(element_oid: Int, text: String) Value!:
	var bytes = text.to_bytes()
	var start = 0

	if bytes.get(0) == 91:
		// A dimension prefix such as `[0:2]=`.
		eq := text.index_of("=").ok_or(bad("array", text))?
		start = eq + 1

	parsed := parse_array(bytes, start, element_oid)?
	return ok(Value.Array(parsed.items))

// An interval's text in the default `postgres` style:
// `1 year 2 mons 3 days 04:05:06.7`.
fn decode_text_interval(text: String) Value!:
	var months = 0
	var days = 0
	var micros = 0
	var amount = 0
	var pending = false

	for word in text.fields():
		if word.contains(":"):
			var sign = 1
			var clock = word.clone()

			if clock.starts_with("-"):
				sign = -1
				clock = clock.slice(1, clock.length())?
			else if clock.starts_with("+"):
				clock = clock.slice(1, clock.length())?

			parts := clock.split(":", none)
			if parts.length() != 3: return error(bad("interval", text))

			hours := parts[0].parse_int().ok_or(bad("interval", text))?
			minutes := parts[1].parse_int().ok_or(bad("interval", text))?
			var whole = parts[2].clone()
			var fraction = 0

			if dot := whole.index_of("."):
				tail := whole.slice(dot + 1, whole.length())?
				digits := tail.pad_end(6, "0")
				head := digits.slice(0, 6)?
				fraction = head.parse_int().ok_or(bad("interval", text))?
				whole = whole.slice(0, dot)?

			seconds := whole.parse_int().ok_or(bad("interval", text))?
			micros += (
				sign
				* (
					(
						(hours * 60 + minutes) * 60
						+ seconds
					) * 1000000
					+ fraction
				)
			)
		else if pending:
			if word.starts_with("year"): months += amount * 12
			else if word.starts_with("mon"): months += amount
			else if word.starts_with("day"): days += amount
			else: return error(bad("interval", text))

			pending = false
		else:
			amount = word.parse_int().ok_or(bad("interval", text))?
			pending = true

	if pending: return error(bad("interval", text))

	return ok(Value.Interval(months, days, micros))

fn decode_text_numeric(text: String) Value:
	parsed := parse_decimal(text)
	if number := parsed: return Value.Decimal(number)

	return Value.Text(text)

// Decodes a column sent in text format into the same `Value` the binary format gives.
fn decode_text(type_oid: Int, text: String) Value!:
	if element := element_of(type_oid): return decode_text_array(element, text)
	if type_oid == JSON or type_oid == JSONB: return ok(Value.Json(text.clone()))
	if type_oid == NUMERIC: return ok(decode_text_numeric(text))
	if type_oid == INTERVAL: return decode_text_interval(text)
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
		or type_oid == JSON
		or type_oid == JSONB
		or type_oid == NUMERIC
		or type_oid == INTERVAL
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

	if type_oid == INTERVAL:
		expect_size("interval", bytes, 16)?
		return ok(
			Value.Interval(
				(bytes.int32_be(12)?).to_int(),
				(bytes.int32_be(8)?).to_int(),
				bytes.int64_be(0)?,
			)
		)

	if type_oid == JSON: return decode_binary_text(JSON, bytes)

	if type_oid == JSONB:
		if bytes.length() == 0 or bytes.get(0) != 1: return error(
			bad("jsonb", "version")
		)
		return decode_binary_text(JSONB, bytes.slice(1, bytes.length())?)

	if type_oid == NUMERIC: return decode_binary_numeric(bytes)

	return decode_binary_text(type_oid, bytes)

fn decode_binary_text(type_oid: Int, bytes: Bytes) Value!:
	text := bytes.decode_utf8().ok_or(
		Error("postgres: column is not valid UTF-8", kind: "EBADMSG")
	)?
	return decode_text(type_oid, text)

// A numeric's binary form: digit count, weight, sign, display scale, then base-10000
// digits. The result is built as decimal text.
fn decode_binary_numeric(bytes: Bytes) Value!:
	if bytes.length() < 8: return error(size_error("numeric", bytes.length()))

	count := (bytes.int16_be(0)?).to_int()
	weight := (bytes.int16_be(2)?).to_int()
	sign := (bytes.uint16_be(4)?).to_int()
	scale := (bytes.int16_be(6)?).to_int()
	if sign == 49152: return ok(Value.Text("NaN"))
	if sign == 53248: return ok(Value.Text("Infinity"))
	if sign == 61440: return ok(Value.Text("-Infinity"))
	if bytes.length() != 8 + 2 * count: return error(
		size_error("numeric", bytes.length())
	)

	var whole = ""

	for j in 0..(weight + 1):
		group := group_at(bytes, count, j)?

		if j == 0: whole = f"{group}"
		else: whole = f"{whole}{f"{group}".pad_start(4, "0")}"

	if whole.is_empty(): whole = "0"

	var fraction = ""
	var j = weight + 1

	while fraction.length() < scale:
		group := group_at(bytes, count, j)?
		fraction = f"{fraction}{f"{group}".pad_start(4, "0")}"
		j += 1

	var text = whole.clone()
	if scale > 0: text = f"{whole}.{fraction.slice(0, scale)?}"
	if sign == 16384: text = f"-{text}"
	return ok(decode_text_numeric(text))

fn group_at(bytes: Bytes, count: Int, index: Int) Int!:
	if index < 0 or index >= count: return ok(0)
	return ok((bytes.int16_be(8 + 2 * index)?).to_int())

// One bound parameter: its bytes (none is NULL) and format code (0 text, 1 binary).
export type Param:
	data: Bytes?
	format: Int

fn text_param(value: Value) Param:
	return Param(encode_value(value), 0)

fn binary_param(bytes: Bytes) Param:
	return Param(bytes, 1)

fn be_integer(value: Int, width: Int) Bytes:
	var out = BytesBuilder()

	if width == 2: out.append_int16_be(value.to_int16().get(0))
	else if width == 4: out.append_int32_be(value.to_int32().get(0))
	else: out.append_int64_be(value)

	return out.build()

fn uuid_bytes(text: String) Bytes?:
	compact := text.replace("-", "")
	if compact.length() != 32: return none

	return hex_decode(compact, none).to_optional()

// Encodes a parameter for a statement whose parameter type is `type_oid`: binary for
// the types with a binary encoder when the value fits, text otherwise (the server then
// parses it or reports the mismatch).
export fn encode_param(type_oid: Int, value: Value) Param:
	match value:
		Value.Null: return Param(none, 0)
		Value.Bool(flag):
			if type_oid == BOOL:
				var out = BytesBuilder()
				out.append(if flag: 1 else: 0)
				return binary_param(out.build())
		Value.Int(number):
			if type_oid == INT8: return binary_param(be_integer(number, 8))
			if type_oid == INT4 and number.to_int32().is_some():
				return binary_param(be_integer(number, 4))
			if type_oid == INT2 and number.to_int16().is_some():
				return binary_param(be_integer(number, 2))
		Value.Float(number):
			var out = BytesBuilder()

			if type_oid == FLOAT8:
				out.append_float64_be(number)
				return binary_param(out.build())
		Value.Bytes(data):
			if type_oid == BYTEA: return binary_param(data.clone())
		Value.Text(text):
			if type_oid == TEXT or type_oid == VARCHAR: return binary_param(
				text.to_bytes()
			)

			if type_oid == UUID:
				if raw := uuid_bytes(text): return binary_param(raw)
		Value.Timestamp(stamp):
			if type_oid == TIMESTAMP or type_oid == TIMESTAMPTZ:
				micros := (
					(stamp.seconds - POSTGRES_EPOCH) * 1000000
					+ stamp.nanos / 1000
				)
				return binary_param(be_integer(micros, 8))
		Value.Date(days):
			if type_oid == DATE: return binary_param(
				be_integer(days - POSTGRES_EPOCH / 86400, 4)
			)
		_: return text_param(value)

	return text_param(value)
