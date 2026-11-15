// Values exchanged with the server. Parameters go out in text format; results come back
// in text or, for the common types, in binary format.
import (
	Bytes, BytesBuilder from "@std/bytes.ms",
	hex_decode, hex_encode from "@std/data/encoding.ms",
	Decimal, parse_decimal from "@std/math/decimal.ms",
	Timestamp, parse_timestamp from "@std/time.ms",
)

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
	// A range: `bounds` holds the lower and upper bound (`Null` when unbounded), and
	// `flags` adds 1 for an inclusive lower bound, 2 for an inclusive upper bound and 4
	// for the empty range. A multirange is an `Array` of ranges.
	Range(bounds: List[Value], flags: Int)
	// `point`, `lseg`, `box`, `path`, `polygon`, `line` or `circle`, as the coordinates
	// the server prints, in order (a `line` is a, b, c; a `circle` is x, y, radius). A
	// `path` is closed when `closed` is set.
	Geometric(kind: String, numbers: List[Float], closed: Bool)
	// An `inet` or `cidr`: the address text and the prefix length in bits.
	Inet(address: String, bits: Int)
	// A `bit` or `varbit`: its length in bits and the bits packed from the high end.
	Bits(length: Int, data: Bytes)
	// A `time`: microseconds since midnight.
	Time(micros: Int)
	// A `timetz`: microseconds since midnight and the UTC offset in seconds, west
	// negative.
	TimeTz(micros: Int, offset: Int)
	// An `hstore`: parallel lists of keys and values (`Null` for an absent value).
	Hstore(keys: List[String], values: List[Value])
	// A composite (row) value: its fields in order.
	Composite(fields: List[Value])

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
		Value.Range(bounds, flags): return range_literal(bounds, flags)
		Value.Geometric(kind, numbers, closed): return geometric_literal(kind, numbers, closed)
		Value.Inet(address, bits): return f"{address}/{bits}"
		Value.Bits(length, data): return bits_literal(length, data)
		Value.Time(micros): return clock_literal(micros)
		Value.TimeTz(micros, offset):
			sign := if offset < 0: "-" else: "+"
			magnitude := if offset < 0: -offset else: offset
			hours := f"{magnitude / 3600}".pad_start(2, "0")
			minutes := f"{magnitude % 3600 / 60}".pad_start(2, "0")
			return f"{clock_literal(micros)}{sign}{hours}:{minutes}"
		Value.Hstore(keys, values):
			var pairs = List[String]()
			for i in 0..keys.length():
				value := values[i].as_text()
				shown := if value.is_none(): "NULL" else: quoted(value.get(""))
				pairs.append(f"{quoted(keys[i])}=>{shown}")
			return pairs.join(", ")
		Value.Composite(fields):
			joined := fields.map(
				fn field: composite_field(field)
			).join(",")
			return f"({joined})"

fn quoted(text: String) String:
	return f"\"{text.replace("\\", "\\\\").replace("\"", "\\\"")}\""

// A composite field: empty for NULL, quoted otherwise.
fn composite_field(field: Value) String:
	text := field.as_text()
	if text.is_none(): return ""
	return quoted(text.get(""))

fn clock_literal(micros: Int) String:
	hours := f"{micros / 3600000000}".pad_start(2, "0")
	minutes := f"{micros / 60000000 % 60}".pad_start(2, "0")
	seconds := f"{micros / 1000000 % 60}".pad_start(2, "0")
	fraction := f"{micros % 1000000}".pad_start(6, "0")
	return f"{hours}:{minutes}:{seconds}.{fraction}"

fn bits_literal(length: Int, data: Bytes) String:
	weights := [128, 64, 32, 16, 8, 4, 2, 1]
	var out = ""

	for i in 0..length:
		byte := data.get(i / 8).to_int()
		bit := byte / weights[i % 8] % 2
		out = f"{out}{bit}"

	return out

fn range_bound(bound: Value) String:
	text := bound.as_text()
	if text.is_none(): return ""
	return quoted(text.get(""))

fn range_literal(bounds: List[Value], flags: Int) String:
	if flags % 8 / 4 == 1: return "empty"

	open := if flags % 2 == 1: "[" else: "("
	close := if flags % 4 / 2 == 1: "]" else: ")"
	return f"{open}{range_bound(bounds[0])},{range_bound(bounds[1])}{close}"

fn points(numbers: List[Float]) List[String]:
	var out = List[String]()
	var i = 0
	while i + 1 < numbers.length():
		out.append(f"({numbers[i]},{numbers[i + 1]})")
		i += 2
	return out

fn geometric_literal(kind: String, numbers: List[Float], closed: Bool) String:
	if kind == "point": return points(numbers).join(",")
	if kind == "box": return points(numbers).join(",")
	if kind == "lseg": return f"[{points(numbers).join(",")}]"
	if kind == "line": return f"\{{numbers[0]},{numbers[1]},{numbers[2]}\}"
	if kind == "circle": return f"<({numbers[0]},{numbers[1]}),{numbers[2]}>"
	if kind == "path" and not closed: return f"[{points(numbers).join(",")}]"
	return f"({points(numbers).join(",")})"

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

