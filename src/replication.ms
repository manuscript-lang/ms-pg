// Streaming replication, physical and logical, over a connection opened with
// `Config.replication` set (`database` for logical, `true` for physical).
import (
	Bytes, BytesBuilder from "@std/bytes.ms",
	parse_int from "@std/math/math.ms",

	Conn, quote_ident from "@pg/conn.ms",
	Value from "@pg/value.ms",
	Backend, copy_data, query from "@pg/wire.ms",
)

fn bad(what: String) Error:
	return Error(f"postgres: {what}", kind: "EPROTO")

// Parses a log sequence number such as `0/16B3748`.
export fn parse_lsn(text: String) Int!:
	parts := text.split("/", none)
	if parts.length() != 2: return error(bad(f"invalid LSN {text}"))

	high := parts[0].parse_int().ok_or(bad(f"invalid LSN {text}"))?
	low := parse_int(parts[1], 16)?
	return ok(high * 4294967296 + low)

export fn format_lsn(lsn: Int) String:
	return f"{(lsn / 4294967296).to_string_radix(16).upper()}/{(lsn % 4294967296).to_string_radix(16).upper()}"

// What `IDENTIFY_SYSTEM` reports.
export type SystemInfo:
	system_id: String
	timeline: Int
	position: Int
	database: String?

export fn identify_system(var conn: Conn) SystemInfo!:
	rows := conn.simple("IDENTIFY_SYSTEM")?
	row := rows.one()?
	database := row[3].text()
	return ok(
		SystemInfo(
			row[0].text().get(""),
			row[1].int().get(0),
			parse_lsn(row[2].text().get(""))?,
			database,
		)
	)

// A replication slot the server created.
export type Slot:
	name: String
	consistent_point: Int
	snapshot: String?

// Creates a logical slot with `plugin` (such as `pgoutput`).
export fn create_logical_slot(
	var conn: Conn,
	name: String,
	plugin: String,
	temporary: Bool,
) Slot!:
	kind := if temporary: " TEMPORARY" else: ""
	rows := conn.simple(
		f"CREATE_REPLICATION_SLOT {quote_ident(name)}{kind} LOGICAL {quote_ident(plugin)}"
	)?
	return slot_from(rows.one()?)

// Creates a physical slot.
export fn create_physical_slot(var conn: Conn, name: String, temporary: Bool) Slot!:
	kind := if temporary: " TEMPORARY" else: ""
	rows := conn.simple(f"CREATE_REPLICATION_SLOT {quote_ident(name)}{kind} PHYSICAL")?
	return slot_from(rows.one()?)

fn slot_from(row: List[Value]) Slot!:
	return ok(
		Slot(
			row[0].text().get(""),
			parse_lsn(row[1].text().get("0/0"))?,
			row[2].text(),
		)
	)

export fn drop_slot(var conn: Conn, name: String) Void!:
	void conn.simple(f"DROP_REPLICATION_SLOT {quote_ident(name)}")?
	return ok()

// Starts streaming from `start` on `slot`. For a logical slot, `options` are the
// plugin's
// `name 'value'` pairs, such as `proto_version '1'`.
export fn start_replication(
	var conn: Conn,
	slot: String,
	start: Int,
	logical: Bool,
	options: List[String],
) Void!:
	kind := if logical: "LOGICAL" else: "PHYSICAL"
	extra := if options.is_empty(): "" else: f" ({options.join(", ")})"
	conn.send(
		query(
			f"START_REPLICATION SLOT {quote_ident(slot)} {kind} {format_lsn(start)}{extra}"
		)?
	)?

	while true:
		message := conn.read()?

		match message:
			Backend.CopyBothResponse: return ok()
			Backend.ErrorResponse(fields): return error(
				Error(
					fields.get_or("M", "replication failed"),
					kind: fields.get_or("C", "XX000"),
				)
			)
			_: return error(bad("START_REPLICATION did not switch to streaming"))

	return ok()

// WAL data: where it starts, where the server's WAL ends, when it was sent
// (microseconds since 2000-01-01) and the bytes (a logical plugin's message, or raw
// WAL).
export type XLogData:
	start: Int
	end: Int
	time: Int
	data: Bytes

// The server's heartbeat; answer with `standby_status` when `reply_requested`.
export type Keepalive:
	end: Int
	time: Int
	reply_requested: Bool

export enum Message:
	XLog(data: XLogData)
	Keepalive(beat: Keepalive)

// Waits for the next WAL data or keepalive.
export fn receive(var conn: Conn) Message!:
	while true:
		message := conn.read()?

		match message:
			Backend.CopyData(body):
				kind := body.get(0)
				if kind == 119:
					return ok(
						Message.XLog(
							XLogData(
								body.int64_be(1)?,
								body.int64_be(9)?,
								body.int64_be(17)?,
								body.slice(
									25,
									body.length(),
								)?,
							)
						)
					)
				if kind == 107:
					return ok(
						Message.Keepalive(
							Keepalive(
								body.int64_be(1)?,
								body.int64_be(9)?,
								body.get(17) != 0,
							)
						)
					)

				return error(bad(f"unknown replication message {kind}"))
			Backend.CopyDone: return error(bad("replication stream ended"))
			Backend.ErrorResponse(fields): return error(
				Error(
					fields.get_or("M", "replication failed"),
					kind: fields.get_or("C", "XX000"),
				)
			)
			_: continue

	return error(bad("unreachable"))

// Reports how far the client has written, flushed and applied the WAL, so the server
// can recycle it. `time` is microseconds since 2000-01-01.
export fn standby_status(
	var conn: Conn,
	written: Int,
	flushed: Int,
	applied: Int,
	time: Int,
	reply: Bool,
) Void!:
	var out = BytesBuilder()
	out.append(114)
	out.append_int64_be(written)
	out.append_int64_be(flushed)
	out.append_int64_be(applied)
	out.append_int64_be(time)
	out.append(if reply: 1 else: 0)
	conn.send(copy_data(out.build())?)?
	return ok()
