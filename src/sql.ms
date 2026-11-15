// Statement text helpers: named arguments and client-side parameter substitution.
import (
	Bytes from "@std/bytes.ms",

	Value from "@pg/value.ms",
)

fn byte_at(bytes: Bytes, i: Int) Int:
	if i >= bytes.length(): return -1
	return bytes.get(i).to_int()

fn span(bytes: Bytes, from: Int, to: Int) String!:
	piece := bytes.slice(from, to)?
	return ok(piece.decode_utf8().get(""))

fn is_word(c: Int) Bool:
	return (c >= 48 and c <= 57) or (c >= 65 and c <= 90) or (c >= 97 and c <= 122) or c == 95

// A statement with `$n` placeholders and the values for them.
export type NamedQuery:
	sql: String
	params: List[Value]

// Visits `sql` outside string literals, quoted names and comments, calling `mark` with
// the byte at each position (and the position); it returns how many bytes it consumed
// (at least 1) and appends what to emit.
fn rewrite(sql: String, mark: fn(Bytes, Int, var List[String]) Int!) String!:
	bytes := sql.to_bytes()
	var out = List[String]()
	var i = 0

	while i < bytes.length():
		c := byte_at(bytes, i)
		var stop = -1

		if c == 39 or c == 34: stop = quoted_end(bytes, i, c)
		else if c == 45 and byte_at(bytes, i + 1) == 45: stop = line_end(bytes, i)
		else if c == 47 and byte_at(bytes, i + 1) == 42: stop = block_end(bytes, i)

		if stop >= 0:
			out.append(span(bytes, i, stop)?)
			i = stop
		else:
			used := mark(bytes, i, var out)?
			i += used

	return ok(out.join(""))

fn quoted_end(bytes: Bytes, at: Int, quote: Int) Int:
	var i = at + 1

	while i < bytes.length():
		if byte_at(bytes, i) == quote:
			if byte_at(bytes, i + 1) == quote:
				i += 2
				continue
			return i + 1
		i += 1

	return bytes.length()

fn line_end(bytes: Bytes, at: Int) Int:
	var i = at
	while i < bytes.length() and byte_at(bytes, i) != 10: i += 1
	return i

fn block_end(bytes: Bytes, at: Int) Int:
	var i = at + 2
	while i < bytes.length() and not (byte_at(bytes, i) == 42 and byte_at(bytes, i + 1) == 47): i += 1
	return (i + 2).min(bytes.length())

fn emit(bytes: Bytes, at: Int, var out: List[String]) Int!:
	out.append(span(bytes, at, at + 1)?)
	return ok(1)

// Turns `@name` placeholders into `$1`, `$2`, ... in order of first use, and collects the
// values from `args`. A name missing from `args` is an error; `@@` stands for a literal
// `@`.
export fn named(sql: String, args: Map[String, Value]) NamedQuery!:
	var order = List[String]()
	var params = List[Value]()

	rewritten := rewrite(
		sql,
		fn(bytes: Bytes, at: Int, var out: List[String]):
			if byte_at(bytes, at) != 64: return emit(bytes, at, var out)

			if byte_at(bytes, at + 1) == 64:
				out.append("@")
				return ok(2)

			var end = at + 1
			while is_word(byte_at(bytes, end)): end += 1
			if end == at + 1: return emit(bytes, at, var out)

			name := span(bytes, at + 1, end)?
			found := args.get(name).ok_or(
				Error(f"postgres: no value for @{name}", kind: "EINVAL")
			)?
			var index = order.index_of(name).get(-1)

			if index < 0:
				order.append(name)
				params.append(found.clone())
				index = order.length() - 1

			out.append(f"${index + 1}")
			return ok(end - at)
	)?

	return ok(NamedQuery(rewritten, params))

fn literal(value: Value) String!:
	text := value.as_text()
	if text.is_none(): return ok("NULL")

	plain := text.get("")
	if plain.contains("\u{0}"): return error(
		Error("postgres: a parameter holds a NUL byte", kind: "EINVAL")
	)

	match value:
		Value.Int(number): return ok(f"{number}")
		Value.Bool(flag): return ok(if flag: "true" else: "false")
		_: return ok(f"'{plain.replace("'", "''")}'")

// Substitutes each `$n` with the literal for `params[n - 1]`, for servers or poolers
// that do not take the extended protocol (pgx's simple protocol mode). Text is quoted
// with doubled single quotes, which assumes `standard_conforming_strings` is on, its
// default. A `$n` with no value is an error.
export fn interpolate(sql: String, params: List[Value]) String!:
	return rewrite(
		sql,
		fn(bytes: Bytes, at: Int, var out: List[String]):
			if byte_at(bytes, at) != 36: return emit(bytes, at, var out)

			var end = at + 1
			while byte_at(bytes, end) >= 48 and byte_at(bytes, end) <= 57: end += 1
			if end == at + 1: return emit(bytes, at, var out)

			digits := span(bytes, at + 1, end)?
			number := digits.parse_int().get(0)
			if number < 1 or number > params.length(): return error(
				Error(f"postgres: no value for ${number}", kind: "EINVAL")
			)

			out.append(literal(params[number - 1])?)
			return ok(end - at)
	)
