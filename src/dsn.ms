// Connection strings and environment variables, in the forms pgx and libpq accept.
import (
	parse_ini from "@std/data/config.ms",
	TlsOptions from "@std/net/tls.ms",
	decode_component from "@std/net/url.ms",
	Duration from "@std/time.ms",

	Config, Endpoint from "@pg/conn.ms",
)

fn invalid(what: String) Error:
	return Error(f"postgres: {what}", kind: "EINVAL")

// Applies one setting. `sslmode` is `disable` or `verify-full` (also `verify-ca`, which
// this client reads as `verify-full`); `prefer`, `allow` and `require` are refused,
// since each either falls back to plaintext or skips certificate verification.
fn apply(var config: Config, key: String, value: String) Void!:
	if key == "host":
		hosts := value.split(",", none)
		config.host = hosts[0].clone()
		config.fallbacks = []
		for i in 1..hosts.length(): config.fallbacks.append(
			Endpoint(hosts[i].clone(), config.port)
		)
	else if key == "port":
		ports := value.split(",", none)
		config.port = ports[0].parse_int().ok_or(invalid(f"invalid port {value}"))?

		for i in 0..config.fallbacks.length():
			text := ports.get(i + 1, ports[0])
			config.fallbacks[i].port = text.parse_int().ok_or(
				invalid(f"invalid port {text}")
			)?
	else if key == "target_session_attrs":
		known := [
			"any", "read-write", "read-only", "primary", "standby", "prefer-standby"
		]
		if not known.contains(value): return error(
			invalid(f"unknown target_session_attrs {value}")
		)

		config.target_session_attrs = value.clone()
	else if key == "trace": config.trace = value == "true"
	else if key == "user": config.user = value.clone()
	else if key == "password": config.password = value.clone()
	else if key == "dbname" or key == "database": config.database = value.clone()
	else if key == "application_name": config.application_name = value.clone()
	else if key == "connect_timeout":
		seconds := value.parse_int().ok_or(invalid(f"invalid connect_timeout {value}"))?
		config.timeout = seconds.seconds()
	else if key == "binary_results": config.binary_results = value != "false"
	else if key == "sslmode":
		if value == "disable": config.tls = none
		else if value == "verify-full" or value == "verify-ca": config.tls = TlsOptions()
		else: return error(
			invalid(
				f"sslmode {value} is not supported; use disable or verify-full"
			)
		)
	else: config.params.add(key.clone(), value.clone())

	return ok()

fn unquote(text: String) String!:
	trimmed := text.trim()

	if trimmed.starts_with("'") and trimmed.ends_with("'") and trimmed.length() >= 2:
		inner := trimmed.slice(1, trimmed.length() - 1)?
		return ok(inner.replace("\\'", "'").replace("\\\\", "\\"))

	return ok(trimmed.clone())

// One `host`, `host:port`, `[v6]` or `[v6]:port` in a URL; port 0 means none given.
fn parse_endpoint(host: String) Endpoint!:
	var name = ""
	var port = 0

	if host.starts_with("["):
		close := host.index_of("]").ok_or(invalid("unterminated IPv6 host"))?
		name = host.slice(1, close)?

		after := host.slice(close + 1, host.length())?

		if after.starts_with(":"):
			digits := after.slice(1, after.length())?
			port = digits.parse_int().ok_or(invalid("invalid port"))?
	else if colon := host.last_index_of(":"):
		name = decode_component(host.slice(0, colon)?)?

		digits := host.slice(colon + 1, host.length())?
		port = digits.parse_int().ok_or(invalid("invalid port"))?
	else:
		name = decode_component(host)?

	return ok(Endpoint(name, port))

// `postgres://user:password@host:port/dbname?sslmode=verify-full&application_name=x`.
// Every part is optional; a percent escape is decoded; a host in square brackets is an
// IPv6 address; an empty host with `?host=/var/run/postgresql` names a Unix socket.
fn parse_url(rest: String) Config!:
	var config = Config()
	var body = rest.clone()
	var query = ""

	if mark := body.index_of("?"):
		query = body.slice(mark + 1, body.length())?
		body = body.slice(0, mark)?

	var path = ""

	if slash := body.index_of("/"):
		path = body.slice(slash + 1, body.length())?
		body = body.slice(0, slash)?

	var hosts = body.clone()

	if at := body.last_index_of("@"):
		login := body.slice(0, at)?
		hosts = body.slice(at + 1, body.length())?

		if colon := login.index_of(":"):
			config.user = decode_component(login.slice(0, colon)?)?
			config.password = decode_component(login.slice(colon + 1, login.length())?)?
		else:
			config.user = decode_component(login)?

	var first = true

	for host in hosts.split(",", none):
		if host.is_empty():
			first = false
			continue

		endpoint := parse_endpoint(host)?

		if first:
			config.host = endpoint.host.clone()
			if endpoint.port != 0: config.port = endpoint.port
		else:
			var other = endpoint.clone()
			if other.port == 0: other.port = config.port
			config.fallbacks.append(other)

		first = false

	if not path.is_empty(): config.database = decode_component(path)?

	for pair in query.split("&", none):
		if pair.is_empty(): continue

		var key = pair.clone()
		var value = ""

		if eq := pair.index_of("="):
			key = pair.slice(0, eq)?
			value = pair.slice(eq + 1, pair.length())?

		apply(var config, decode_component(key)?, decode_component(value)?)?

	return ok(config)

// `host=localhost port=5432 user=app password='a b' dbname=app sslmode=verify-full`.
fn parse_keywords(text: String) Config!:
	var config = Config()
	var rest = text.trim().clone()

	while not rest.is_empty():
		eq := rest.index_of("=").ok_or(invalid(f"expected key=value in {rest}"))?
		key_text := rest.slice(0, eq)?
		key := key_text.trim().clone()
		after_text := rest.slice(eq + 1, rest.length())?
		after := after_text.trim_start().clone()
		var value = ""

		if after.starts_with("'"):
			var at = 1

			while at < after.length():
				here := after.slice(at, at + 1)?

				if here == "\\": at += 2
				else if here == "'": break
				else: at += 1

			if at >= after.length(): return error(invalid("unterminated quote"))

			value = unquote(after.slice(0, at + 1)?)?

			tail := after.slice(at + 1, after.length())?
			rest = tail.trim_start().clone()
		else:
			end := after.index_any(" \t\n").get(after.length())
			value = after.slice(0, end)?

			tail := after.slice(end, after.length())?
			rest = tail.trim_start().clone()

		apply(var config, key, value)?

	return ok(config)

// Reads a connection string: a `postgres://` or `postgresql://` URL, or `key=value`
// pairs separated by spaces. Unknown keys become server settings in `Config.params`.
export fn parse_config(text: String) Config!:
	if text.starts_with("postgres://"): return parse_url(text.slice(11, text.length())?)
	if text.starts_with("postgresql://"): return parse_url(
		text.slice(13, text.length())?
	)

	return parse_keywords(text)

// Reads the libpq environment variables from `environment()`: PGHOST, PGPORT, PGUSER,
// PGPASSWORD, PGDATABASE, PGAPPNAME, PGCONNECT_TIMEOUT and PGSSLMODE.
export fn config_from_env(env: Map[String, String]) Config!:
	var config = Config()

	for entry in [
		"PGHOST=host", "PGPORT=port", "PGUSER=user", "PGPASSWORD=password",
		"PGDATABASE=dbname", "PGAPPNAME=application_name",
		"PGCONNECT_TIMEOUT=connect_timeout", "PGSSLMODE=sslmode"
	]:
		parts := entry.split("=", none)
		if value := env.get(parts[0]): apply(var config, parts[1], value)?

	return ok(config)

// Splits a `.pgpass` line on colons, honouring `\:` and `\\`.
fn pgpass_fields(line: String) List[String]:
	var fields = List[String]()
	var current = ""
	var escaped = false

	for i in 0..line.length():
		c := line.slice(i, i + 1).get("")

		if escaped:
			current = f"{current}{c}"
			escaped = false
		else if c == "\\": escaped = true
		else if c == ":":
			fields.append(current)
			current = ""
		else: current = f"{current}{c}"

	fields.append(current)
	return fields

// Finds the password for a server in the text of a `.pgpass` file: lines of
// `host:port:database:user:password`, each field `*` to match anything, `#` for
// comments. The first matching line wins. Read the file with `@std/os/fs.ms`; this
// package reads no files.
export fn pgpass_password(
	text: String,
	host: String,
	port: Int,
	database: String,
	user: String,
) String?:
	wanted := [host, f"{port}", database, user]

	for line in text.split("\n", none):
		if line.is_empty() or line.starts_with("#"): continue

		fields := pgpass_fields(line)
		if fields.length() != 5: continue

		var matches = true
		for i in 0..4:
			if fields[i] != "*" and fields[i] != wanted[i]: matches = false
		if matches: return fields[4].clone()

	return none

// Reads one `[name]` section of a `pg_service.conf` file; the keys are those of a
// connection string.
export fn parse_service(text: String, name: String) Config!:
	sections := parse_ini(text)?
	section := sections.get(name).ok_or(invalid(f"no service {name}"))?
	var config = Config()
	for entry in section: apply(var config, entry.key.clone(), entry.value.clone())?
	return ok(config)
