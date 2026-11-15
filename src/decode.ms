// Decoding result columns and encoding bound parameters, by type OID and format.
import (
	Bytes, BytesBuilder from "@std/bytes.ms",
	hex_decode, hex_encode from "@std/data/encoding.ms",
	parse_decimal from "@std/math/decimal.ms",
	Timestamp, parse_timestamp from "@std/time.ms",

	Value, encode_value from "@pg/value.ms",
)

// A type the server defines by name (an enum, a composite, a domain, an extension type),
// registered with `Conn.load_types` so its columns can be decoded.
export type TypeInfo:
	// `enum`, `composite`, `domain`, `range`, `multirange`, `array` or `hstore`.
	kind: String
	// The element of an array, the subtype of a range or multirange, or the base of a
	// domain.
	element: Int = 0
	// The field types of a composite, in order.
	fields: List[Int] = []

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
TIME: Int := 1083
TIMETZ: Int := 1266
INET: Int := 869
CIDR: Int := 650
MACADDR: Int := 829
MACADDR8: Int := 774
BIT: Int := 1560
VARBIT: Int := 1562
POINT: Int := 600
LSEG: Int := 601
PATH: Int := 602
BOX: Int := 603
POLYGON: Int := 604
LINE: Int := 628
CIRCLE: Int := 718
// Seconds from 1970-01-01 to 2000-01-01, PostgreSQL's epoch.
POSTGRES_EPOCH: Int := 946684800

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
	if type_oid == 1017: return POINT
	if type_oid == 1018: return LSEG
	if type_oid == 1019: return PATH
	if type_oid == 1020: return BOX
	if type_oid == 1027: return POLYGON
	if type_oid == 629: return LINE
	if type_oid == 719: return CIRCLE
	if type_oid == 1041: return INET
	if type_oid == 651: return CIDR
	if type_oid == 1040: return MACADDR
	if type_oid == 775: return MACADDR8
	if type_oid == 1183: return TIME
	if type_oid == 1270: return TIMETZ
	if type_oid == 1561: return BIT
	if type_oid == 1563: return VARBIT
	if type_oid == 3905: return 3904
	if type_oid == 3907: return 3906
	if type_oid == 3909: return 3908
	if type_oid == 3911: return 3910
	if type_oid == 3913: return 3912
	if type_oid == 3927: return 3926
	if type_oid == 1014: return 1042
	if type_oid == 1003: return 19

	return none

type ArrayPart:
	items: List[Value]
	next: Int

// Reads the array whose `{` is at byte `at`, and returns the byte after its `}`.
fn parse_array(bytes: Bytes, at: Int, element_oid: Int, types: Map[Int, TypeInfo]) ArrayPart!:
	var items = List[Value]()
	var i = at + 1
	if bytes.get(i) == 125: return ok(ArrayPart(items, i + 1))

	while true:
		here := bytes.get(i)

		if here == 123:
			inner := parse_array(bytes, i, element_oid, types)?
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
			items.append(decode_text(element_oid, text, types)?)
		else:
			start := i
			while bytes.get(i) != 44 and bytes.get(i) != 125: i += 1

			piece := bytes.slice(start, i)?
			text := piece.decode_utf8().ok_or(bad("array", "invalid UTF-8"))?

			if text.equal_fold("NULL"): items.append(Value.Null)
			else: items.append(decode_text(element_oid, text, types)?)

		if bytes.get(i) == 125: return ok(ArrayPart(items, i + 1))
		if bytes.get(i) != 44: return error(bad("array", "expected a comma"))

		i += 1

	return error(bad("array", "unterminated"))

fn decode_text_array(element_oid: Int, text: String, types: Map[Int, TypeInfo]) Value!:
	var bytes = text.to_bytes()
	var start = 0

	if bytes.get(0) == 91:
		// A dimension prefix such as `[0:2]=`.
		eq := text.index_of("=").ok_or(bad("array", text))?
		start = eq + 1

	parsed := parse_array(bytes, start, element_oid, types)?
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

// ---- Ranges, geometry, network, bit strings, time, hstore and composites ----

// The byte at `i`, or -1 past the end.
fn byte_at(bytes: Bytes, i: Int) Int:
	if i >= bytes.length(): return -1
	return bytes.get(i).to_int()

type Piece:
	text: String
	quoted: Bool
	next: Int

// Reads a quoted piece (backslash escapes and doubled quotes) or an unquoted one up to
// a byte in `stops`.
fn read_piece(bytes: Bytes, at: Int, stops: List[Int]) Piece!:
	var raw = BytesBuilder()
	var i = at

	if byte_at(bytes, i) == 34:
		i += 1

		while byte_at(bytes, i) != 34:
			c := byte_at(bytes, i)
			if c < 0: return error(bad("value", "unterminated quote"))

			if c == 92:
				i += 1
				raw.append(bytes.get(i))
				i += 1
			else:
				raw.append(bytes.get(i))
				i += 1

			if byte_at(bytes, i) == 34 and byte_at(bytes, i + 1) == 34:
				raw.append(34)
				i += 2

		text := raw.build().decode_utf8().ok_or(bad("value", "invalid UTF-8"))?
		return ok(Piece(text, true, i + 1))

	while byte_at(bytes, i) >= 0 and not stops.contains(byte_at(bytes, i)):
		raw.append(bytes.get(i))
		i += 1

	text := raw.build().decode_utf8().ok_or(bad("value", "invalid UTF-8"))?
	return ok(Piece(text, false, i))

// The subtype of a range type: the built-in ranges, or one registered with `load_types`.
fn range_subtype(type_oid: Int, types: Map[Int, TypeInfo]) Int?:
	if type_oid == 3904: return INT4
	if type_oid == 3906: return NUMERIC
	if type_oid == 3908: return TIMESTAMP
	if type_oid == 3910: return TIMESTAMPTZ
	if type_oid == 3912: return DATE
	if type_oid == 3926: return INT8

	if known := types.get(type_oid):
		if known.kind == "range": return known.element

	return none

fn multirange_subtype(type_oid: Int, types: Map[Int, TypeInfo]) Int?:
	if type_oid == 4451: return INT4
	if type_oid == 4532: return NUMERIC
	if type_oid == 4533: return TIMESTAMP
	if type_oid == 4534: return TIMESTAMPTZ
	if type_oid == 4535: return DATE
	if type_oid == 4536: return INT8

	if known := types.get(type_oid):
		if known.kind == "multirange": return known.element

	return none

type ValuePart:
	value: Value
	next: Int

fn bound_value(piece: Piece, subtype: Int, types: Map[Int, TypeInfo]) Value!:
	if piece.text.is_empty() and not piece.quoted: return ok(Value.Null)
	return decode_text(subtype, piece.text, types)

// Reads one range such as `[1,5)`, `(,3]` or `empty` starting at `at`.
fn parse_range(bytes: Bytes, at: Int, subtype: Int, types: Map[Int, TypeInfo]) ValuePart!:
	if byte_at(bytes, at) == 101:
		return ok(ValuePart(Value.Range([Value.Null, Value.Null], 4), at + 5))

	var flags = 0
	if byte_at(bytes, at) == 91: flags += 1

	lower := read_piece(bytes, at + 1, [44])?
	upper := read_piece(bytes, lower.next + 1, [41, 93])?
	if byte_at(bytes, upper.next) == 93: flags += 2

	after := upper.next + 1
	low := bound_value(lower, subtype, types)?
	high := bound_value(upper, subtype, types)?
	return ok(ValuePart(Value.Range([low, high], flags), after))

fn decode_text_range(subtype: Int, text: String, types: Map[Int, TypeInfo]) Value!:
	part := parse_range(text.to_bytes(), 0, subtype, types)?
	return ok(part.value)

fn decode_text_multirange(subtype: Int, text: String, types: Map[Int, TypeInfo]) Value!:
	bytes := text.to_bytes()
	var items = List[Value]()
	var i = 1

	while byte_at(bytes, i) != 125:
		part := parse_range(bytes, i, subtype, types)?
		items.append(part.value)
		i = part.next
		if byte_at(bytes, i) == 44: i += 1

	return ok(Value.Array(items))

// Reads a composite such as `(1,"a b",)`.
fn decode_text_composite(oids: List[Int], text: String, types: Map[Int, TypeInfo]) Value!:
	bytes := text.to_bytes()
	var fields = List[Value]()
	var i = 1
	var index = 0

	while byte_at(bytes, i) >= 0:
		piece := read_piece(bytes, i, [44, 41])?

		if piece.text.is_empty() and not piece.quoted:
			fields.append(Value.Null)
		else:
			oid := oids.get(index, TEXT)
			fields.append(decode_text(oid, piece.text, types)?)

		index += 1
		i = piece.next + 1
		if byte_at(bytes, piece.next) != 44: break

	return ok(Value.Composite(fields))

// Reads an hstore such as `"a"=>"1", "b"=>NULL`.
fn decode_text_hstore(text: String) Value!:
	bytes := text.to_bytes()
	var keys = List[String]()
	var values = List[Value]()
	var i = 0

	while byte_at(bytes, i) >= 0:
		while byte_at(bytes, i) == 32 or byte_at(bytes, i) == 44: i += 1
		if byte_at(bytes, i) < 0: break

		key := read_piece(bytes, i, [61])?
		keys.append(key.text.clone())
		i = key.next + 2

		value := read_piece(bytes, i, [44])?
		if not value.quoted and value.text == "NULL": values.append(Value.Null)
		else: values.append(Value.Text(value.text.clone()))
		i = value.next

	return ok(Value.Hstore(keys, values))

fn geometry_kind(type_oid: Int) String?:
	if type_oid == POINT: return "point"
	if type_oid == LSEG: return "lseg"
	if type_oid == PATH: return "path"
	if type_oid == BOX: return "box"
	if type_oid == POLYGON: return "polygon"
	if type_oid == LINE: return "line"
	if type_oid == CIRCLE: return "circle"
	return none

fn decode_text_geometry(kind: String, text: String) Value!:
	flat := text.map_chars(
		fn c: if "()[]{}<>,".contains_any(c): " " else: c
	)
	var numbers = List[Float]()

	for word in flat.fields():
		numbers.append(word.parse_float().ok_or(bad(kind, text))?)

	closed := kind == "polygon" or kind == "path" and text.starts_with("(")
	return ok(Value.Geometric(kind, numbers, closed))

fn decode_text_inet(text: String) Value!:
	if slash := text.index_of("/"):
		address := text.slice(0, slash)?
		digits := text.slice(slash + 1, text.length())?
		bits := digits.parse_int().ok_or(bad("inet", text))?
		return ok(Value.Inet(address, bits))

	return ok(Value.Inet(text.clone(), if text.contains(":"): 128 else: 32))

fn decode_text_bits(text: String) Value!:
	var out = BytesBuilder()
	var byte = 0
	var count = 0

	for i in 0..text.length():
		digit := text.slice(i, i + 1)?
		byte = byte * 2 + (if digit == "1": 1 else: 0)
		count += 1

		if count % 8 == 0:
			out.append(byte.to_uint8().get(0))
			byte = 0

	if count % 8 != 0:
		weights := [1, 2, 4, 8, 16, 32, 64, 128]
		out.append((byte * weights[8 - count % 8]).to_uint8().get(0))

	return ok(Value.Bits(text.length(), out.build()))

// `HH:MM:SS[.ffffff]` as microseconds.
fn clock_micros(text: String) Int!:
	parts := text.split(":", none)
	if parts.length() != 3: return error(bad("time", text))

	hours := parts[0].parse_int().ok_or(bad("time", text))?
	minutes := parts[1].parse_int().ok_or(bad("time", text))?
	var whole = parts[2].clone()
	var fraction = 0

	if dot := whole.index_of("."):
		tail := whole.slice(dot + 1, whole.length())?
		head := tail.pad_end(6, "0").slice(0, 6)?
		fraction = head.parse_int().ok_or(bad("time", text))?
		whole = whole.slice(0, dot)?

	seconds := whole.parse_int().ok_or(bad("time", text))?
	return ok(((hours * 60 + minutes) * 60 + seconds) * 1000000 + fraction)

fn decode_text_time(text: String) Value!:
	return ok(Value.Time(clock_micros(text)?))

// `HH:MM:SS+HH[:MM[:SS]]`; the offset is east of UTC in the text and kept that way.
fn decode_text_timetz(text: String) Value!:
	var cut = text.length()

	for i in 1..text.length():
		c := text.slice(i, i + 1)?
		if c == "+" or c == "-":
			cut = i
			break

	if cut == text.length(): return error(bad("timetz", text))

	micros := clock_micros(text.slice(0, cut)?)?
	zone := text.slice(cut, text.length())?
	sign := if zone.starts_with("-"): -1 else: 1
	offset := zone.slice(1, zone.length())?
	parts := offset.split(":", none)
	var seconds = parts[0].parse_int().ok_or(bad("timetz", text))? * 3600
	if parts.length() > 1: seconds += parts[1].parse_int().ok_or(bad("timetz", text))? * 60
	if parts.length() > 2: seconds += parts[2].parse_int().ok_or(bad("timetz", text))?
	return ok(Value.TimeTz(micros, sign * seconds))

// Text decoding of the types above and of registered types; none when the OID is
// neither.
fn decode_text_extra(type_oid: Int, text: String, types: Map[Int, TypeInfo]) Value?!:
	if subtype := range_subtype(type_oid, types): return ok(decode_text_range(subtype, text, types)?)
	if subtype := multirange_subtype(type_oid, types): return ok(decode_text_multirange(subtype, text, types)?)
	if kind := geometry_kind(type_oid): return ok(decode_text_geometry(kind, text)?)
	if type_oid == INET or type_oid == CIDR: return ok(decode_text_inet(text)?)
	if type_oid == BIT or type_oid == VARBIT: return ok(decode_text_bits(text)?)
	if type_oid == TIME: return ok(decode_text_time(text)?)
	if type_oid == TIMETZ: return ok(decode_text_timetz(text)?)

	if known := types.get(type_oid):
		if known.kind == "composite": return ok(decode_text_composite(known.fields, text, types)?)
		if known.kind == "hstore": return ok(decode_text_hstore(text)?)
		if known.kind == "domain": return ok(decode_text(known.element, text, types)?)
		if known.kind == "array": return ok(decode_text_array(known.element, text, types)?)

	return ok(none)

// Decodes a column sent in text format into the same `Value` the binary format gives.
fn decode_text(type_oid: Int, text: String, types: Map[Int, TypeInfo]) Value!:
	if extra := decode_text_extra(type_oid, text.clone(), types)?: return ok(extra)
	if element := element_of(type_oid): return decode_text_array(element, text, types)
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
export fn decode_value(type_oid: Int, format: Int, raw: Bytes?, types: Map[Int, TypeInfo]) Value!:
	if raw.is_none(): return ok(Value.Null)

	bytes := raw.get(Bytes([]))
	if format == 1: return decode_binary(type_oid, bytes, types)

	text := bytes.decode_utf8().ok_or(
		Error("postgres: column is not valid UTF-8", kind: "EBADMSG")
	)?
	return decode_text(type_oid, text, types)

// Whether `type_oid` has a binary decoder here; other columns stay in text format.
export fn is_binary_type(type_oid: Int, types: Map[Int, TypeInfo]) Bool:
	if element := element_of(type_oid): return is_binary_type(element, types)
	if subtype := range_subtype(type_oid, types): return is_binary_type(subtype, types)
	if subtype := multirange_subtype(type_oid, types): return is_binary_type(subtype, types)
	if geometry_kind(type_oid).is_some(): return true

	if known := types.get(type_oid):
		if known.kind == "array" or known.kind == "domain": return is_binary_type(known.element, types)
		if known.kind == "composite": return known.fields.all(fn oid: is_binary_type(oid, types))
		return known.kind == "enum" or known.kind == "hstore"

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
		or type_oid == TIME
		or type_oid == TIMETZ
		or type_oid == INET
		or type_oid == CIDR
		or type_oid == MACADDR
		or type_oid == MACADDR8
		or type_oid == BIT
		or type_oid == VARBIT
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
fn decode_binary(type_oid: Int, bytes: Bytes, types: Map[Int, TypeInfo]) Value!:
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

	if extra := decode_binary_extra(type_oid, bytes, types)?: return ok(extra)

	if type_oid == JSON: return decode_binary_text(JSON, bytes, types)

	if type_oid == JSONB:
		if bytes.length() == 0 or bytes.get(0) != 1: return error(
			bad("jsonb", "version")
		)
		return decode_binary_text(JSONB, bytes.slice(1, bytes.length())?, types)

	if type_oid == NUMERIC: return decode_binary_numeric(bytes)

	return decode_binary_text(type_oid, bytes, types)

// ---- Binary forms of the types above ----

fn int32_at(bytes: Bytes, at: Int) Int!:
	return ok((bytes.int32_be(at)?).to_int())

// Elements of one array dimension, then the nested ones for the next dimension.
fn binary_dimension(
	bytes: Bytes,
	lengths: List[Int],
	level: Int,
	at: Int,
	element_oid: Int,
	types: Map[Int, TypeInfo],
) ValuePart!:
	var items = List[Value]()
	var i = at

	for _n in 0..lengths[level]:
		if level + 1 < lengths.length():
			inner := binary_dimension(bytes, lengths, level + 1, i, element_oid, types)?
			items.append(inner.value.clone())
			i = inner.next
		else:
			size := int32_at(bytes, i)?
			i += 4

			if size < 0:
				items.append(Value.Null)
			else:
				items.append(decode_binary(element_oid, bytes.slice(i, i + size)?, types)?)
				i += size

	return ok(ValuePart(Value.Array(items), i))

fn decode_binary_array(bytes: Bytes, types: Map[Int, TypeInfo]) Value!:
	dimensions := int32_at(bytes, 0)?
	if dimensions == 0: return ok(Value.Array([]))

	element_oid := (bytes.uint32_be(8)?).to_int()
	var lengths = List[Int]()
	for d in 0..dimensions: lengths.append(int32_at(bytes, 12 + 8 * d)?)

	parsed := binary_dimension(bytes, lengths, 0, 12 + 8 * dimensions, element_oid, types)?
	return ok(parsed.value)

fn decode_binary_range(subtype: Int, bytes: Bytes, types: Map[Int, TypeInfo]) Value!:
	flag := bytes.get(0).to_int()
	if flag % 2 == 1: return ok(Value.Range([Value.Null, Value.Null], 4))

	var flags = 0
	if flag / 2 % 2 == 1: flags += 1
	if flag / 4 % 2 == 1: flags += 2

	var i = 1
	var bounds = List[Value]()

	for infinite_bit in [8, 16]:
		if flag / infinite_bit % 2 == 1:
			bounds.append(Value.Null)
		else:
			size := int32_at(bytes, i)?
			bounds.append(decode_binary(subtype, bytes.slice(i + 4, i + 4 + size)?, types)?)
			i += 4 + size

	return ok(Value.Range(bounds, flags))

fn decode_binary_multirange(subtype: Int, bytes: Bytes, types: Map[Int, TypeInfo]) Value!:
	count := int32_at(bytes, 0)?
	var items = List[Value]()
	var i = 4

	for _n in 0..count:
		size := int32_at(bytes, i)?
		items.append(decode_binary_range(subtype, bytes.slice(i + 4, i + 4 + size)?, types)?)
		i += 4 + size

	return ok(Value.Array(items))

fn decode_binary_composite(bytes: Bytes, types: Map[Int, TypeInfo]) Value!:
	count := int32_at(bytes, 0)?
	var fields = List[Value]()
	var i = 4

	for _n in 0..count:
		oid := (bytes.uint32_be(i)?).to_int()
		size := int32_at(bytes, i + 4)?
		i += 8

		if size < 0:
			fields.append(Value.Null)
		else:
			fields.append(decode_binary(oid, bytes.slice(i, i + size)?, types)?)
			i += size

	return ok(Value.Composite(fields))

fn decode_binary_hstore(bytes: Bytes) Value!:
	count := int32_at(bytes, 0)?
	var keys = List[String]()
	var values = List[Value]()
	var i = 4

	for _n in 0..count:
		key_size := int32_at(bytes, i)?
		key_bytes := bytes.slice(i + 4, i + 4 + key_size)?
		key := key_bytes.decode_utf8().ok_or(bad("hstore", "key"))?
		keys.append(key)
		i += 4 + key_size

		size := int32_at(bytes, i)?
		i += 4

		if size < 0:
			values.append(Value.Null)
		else:
			value_bytes := bytes.slice(i, i + size)?
			text := value_bytes.decode_utf8().ok_or(bad("hstore", "value"))?
			values.append(Value.Text(text))
			i += size

	return ok(Value.Hstore(keys, values))

fn float_at(bytes: Bytes, at: Int) Float!:
	return bytes.float64_be(at)

fn floats(bytes: Bytes, from: Int, count: Int) List[Float]!:
	var out = List[Float]()
	for n in 0..count: out.append(float_at(bytes, from + 8 * n)?)
	return ok(out)

fn decode_binary_geometry(kind: String, bytes: Bytes) Value!:
	if kind == "point": return ok(Value.Geometric(kind, floats(bytes, 0, 2)?, false))
	if kind == "lseg" or kind == "box": return ok(Value.Geometric(kind, floats(bytes, 0, 4)?, false))
	if kind == "line" or kind == "circle": return ok(Value.Geometric(kind, floats(bytes, 0, 3)?, false))

	if kind == "path":
		count := int32_at(bytes, 1)?
		return ok(Value.Geometric(kind, floats(bytes, 5, count * 2)?, bytes.get(0) != 0))

	count := int32_at(bytes, 0)?
	return ok(Value.Geometric(kind, floats(bytes, 4, count * 2)?, true))

// An IPv6 address as the server prints it: lowercase hex groups, the longest run of
// zero groups collapsed to `::`.
fn ipv6_text(groups: List[Int]) String:
	var best_start = -1
	var best_length = 0
	var i = 0

	while i < 8:
		if groups[i] == 0:
			var j = i
			while j < 8 and groups[j] == 0: j += 1
			if j - i > best_length and j - i >= 2:
				best_start = i
				best_length = j - i
			i = j
		else:
			i += 1

	if best_start < 0:
		var all = List[String]()
		for g in groups: all.append(g.to_string_radix(16))
		return all.join(":")

	var head = List[String]()
	var tail = List[String]()
	for g in 0..best_start: head.append(groups[g].to_string_radix(16))
	for g in (best_start + best_length)..8: tail.append(groups[g].to_string_radix(16))
	return f"{head.join(":")}::{tail.join(":")}"

fn decode_binary_inet(bytes: Bytes) Value!:
	family := bytes.get(0).to_int()
	bits := bytes.get(1).to_int()

	if family == 2:
		parts := [bytes.get(4), bytes.get(5), bytes.get(6), bytes.get(7)]
		return ok(Value.Inet(parts.map(fn b: f"{b}").join("."), bits))

	var groups = List[Int]()
	for g in 0..8: groups.append(bytes.get(4 + 2 * g).to_int() * 256 + bytes.get(5 + 2 * g).to_int())
	return ok(Value.Inet(ipv6_text(groups), bits))

fn decode_binary_bits(bytes: Bytes) Value!:
	length := int32_at(bytes, 0)?
	return ok(Value.Bits(length, bytes.slice(4, bytes.length())?))

fn decode_binary_macaddr(bytes: Bytes) Value:
	var parts = List[String]()
	for b in bytes.values(): parts.append(b.to_int().to_string_radix(16).pad_start(2, "0"))
	return Value.Text(parts.join(":"))

// Binary decoding of the types above and of registered types; none when the OID is
// neither.
fn decode_binary_extra(type_oid: Int, bytes: Bytes, types: Map[Int, TypeInfo]) Value?!:
	if element_of(type_oid).is_some(): return ok(decode_binary_array(bytes, types)?)
	if subtype := range_subtype(type_oid, types): return ok(decode_binary_range(subtype, bytes, types)?)
	if subtype := multirange_subtype(type_oid, types): return ok(decode_binary_multirange(subtype, bytes, types)?)
	if kind := geometry_kind(type_oid): return ok(decode_binary_geometry(kind, bytes)?)
	if type_oid == INET or type_oid == CIDR: return ok(decode_binary_inet(bytes)?)
	if type_oid == BIT or type_oid == VARBIT: return ok(decode_binary_bits(bytes)?)
	if type_oid == MACADDR or type_oid == MACADDR8: return ok(decode_binary_macaddr(bytes))
	if type_oid == TIME: return ok(Value.Time(bytes.int64_be(0)?))
	if type_oid == TIMETZ: return ok(Value.TimeTz(bytes.int64_be(0)?, -int32_at(bytes, 8)?))

	if known := types.get(type_oid):
		if known.kind == "array": return ok(decode_binary_array(bytes, types)?)
		if known.kind == "composite": return ok(decode_binary_composite(bytes, types)?)
		if known.kind == "hstore": return ok(decode_binary_hstore(bytes)?)
		if known.kind == "domain": return ok(decode_binary(known.element, bytes, types)?)
		if known.kind == "enum":
			text := bytes.decode_utf8().ok_or(bad("enum", "invalid UTF-8"))?
			return ok(Value.Text(text))

	return ok(none)

fn decode_binary_text(type_oid: Int, bytes: Bytes, types: Map[Int, TypeInfo]) Value!:
	text := bytes.decode_utf8().ok_or(
		Error("postgres: column is not valid UTF-8", kind: "EBADMSG")
	)?
	return decode_text(type_oid, text, types)

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
