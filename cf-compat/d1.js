// Cloudflare D1 API emulation over PostgreSQL (node-postgres).
//
// Source.js keeps its original D1 call pattern untouched:
//   env.DB.prepare(sql).bind(...values).run() | .first() | .all()
//   env.DB.batch([stmt, ...])
//
// Only SQLite/D1-specific syntax is translated here (never business logic):
//   - `INTEGER PRIMARY KEY AUTOINCREMENT`      -> `SERIAL PRIMARY KEY`
//   - `PRAGMA table_info(x)`                   -> information_schema query
//   - `<col> = ? COLLATE NOCASE`               -> `lower(<col>) = lower(?)`
//   - `INSERT OR REPLACE INTO t (...) VALUES..`-> `INSERT .. ON CONFLICT (<pk>) DO UPDATE`
//
// Result fidelity notes:
//   - `.all()` returns `{ results }`, `.first()` returns row|null (same as D1).
//   - Timestamps are returned verbatim as TEXT-like strings (timestamp OID 1114
//     parser passthrough) to mirror D1's string storage of `created_at`.
import pg from "pg";

// Preserve raw timestamp strings (matches how D1 stored CURRENT_TIMESTAMP).
// Map int8 (BIGINT) to JS numbers: epoch-ms columns are 64-bit in SQLite/D1
// and would overflow a 32-bit int4; pg returns BIGINT as string by default.
try {
	pg.types.setTypeParser(1114, (v) => v);
	pg.types.setTypeParser(20, (v) => (v === null ? null : Number(v)));
} catch (e) { }

const FALLBACK_PK = { settings: ["key"] };
const pkCache = new Map();

// Compiled-statement cache: _translate + INSERT-OR-REPLACE rewriting +
// placeholder conversion are pure functions of the SQL text, and the same
// handful of statements run on every request. Key -> {text, params?};
// `params` present only when the compiled form carries its own (PRAGMA).
const compileCache = new Map();

function coerceParam(v) {
	if (v === undefined) return null;
	if (v instanceof ArrayBuffer) return Buffer.from(v);
	if (ArrayBuffer.isView(v)) return Buffer.from(v.buffer, v.byteOffset, v.byteLength);
	return v;
}

// SQLite/D1 use `?` placeholders; node-postgres uses `$1..$n`.
// Converts sequentially while skipping single-quoted string literals.
function convertPlaceholders(sql) {
	let out = "";
	let n = 0;
	let inString = false;
	for (let i = 0; i < sql.length; i++) {
		const ch = sql[i];
		if (inString) {
			out += ch;
			if (ch === "'") {
				if (sql[i + 1] === "'") {
					out += "'";
					i++;
				} else {
					inString = false;
				}
			}
			continue;
		}
		if (ch === "'") {
			inString = true;
			out += ch;
			continue;
		}
		if (ch === "?") {
			n++;
			out += "$" + n;
			continue;
		}
		out += ch;
	}
	return out;
}

class D1PreparedStatement {
	constructor(db, sql, params = []) {
		this.db = db;
		this.sqlText = String(sql);
		this.params = params;
	}
	bind(...values) {
		return new D1PreparedStatement(this.db, this.sqlText, values.map(coerceParam));
	}
	async run() {
		const meta = await this.db._execute(this.sqlText, this.params);
		return { success: true, meta: { changes: meta.rowCount ?? 0, duration: 0 } };
	}
	async first(columnName) {
		const { rows } = await this.db._query(this.sqlText, this.params);
		const row = rows.length > 0 ? rows[0] : null;
		if (row === null || columnName === undefined || columnName === null) return row;
		return row[columnName] !== undefined ? row[columnName] : null;
	}
	async all() {
		const { rows } = await this.db._query(this.sqlText, this.params);
		return { results: rows };
	}

	raw(options = {}) {
		return this.db._query(this.sqlText, this.params).then(({ rows }) => {
			if (!rows.length) return [];
			const cols = Object.keys(rows[0]);
			const data = rows.map((r) => cols.map((c) => r[c]));
			return options.columnNames ? [cols, ...data] : data;
		});
	}
}

export class D1Database {
	constructor(connectionString, opts = {}) {
		if (!connectionString) {
			// Mirror Workers behavior: requests fail per-request instead of crash-looping.
			this.pool = null;
			this.missingUrl = true;
			return;
		}
		this.missingUrl = false;
		const parsed = (() => {
			try {
				return new URL(connectionString);
			} catch (e) {
				return null;
			}
		})();
		let ssl;
		if (process.env.DATABASE_SSL === "true") ssl = { rejectUnauthorized: false };
		else if (
			parsed &&
			((parsed.searchParams && parsed.searchParams.get("sslmode") && parsed.searchParams.get("sslmode") !== "disable") ||
				/\.render\.com$/i.test(parsed.hostname))
		) {
			ssl = { rejectUnauthorized: false };
		}
		this.pool = new pg.Pool({
			connectionString,
			max: Number(opts.max || process.env.PG_POOL_MAX || 10),
			idleTimeoutMillis: 30000,
			connectionTimeoutMillis: Number(process.env.PG_CONNECT_TIMEOUT_MS || 10000),
			// Managed Postgres providers (Render, Heroku, pgbouncer) drop idle
			// TCP connections; without keep-alive probes pooled clients go stale
			// and requests intermittently fail with "terminating connection".
			keepAlive: true,
			keepAliveInitialDelayMillis: 10000,
			ssl,
		});
		this.pool.on("error", () => { });
	}

	get connected() {
		return !!this.pool;
	}

	async ping() {
		if (!this.pool) throw new Error("DATABASE_URL is not configured");
		await this.pool.query("SELECT 1");
	}

	// One-time widening migration for databases created before the int4->int8
	// fix (CREATE TABLE IF NOT EXISTS never alters existing columns). Idempotent.
	async migrate() {
		if (!this.pool) return;
		const bigintCols = [
			"last_active",
			"first_connection_time",
			"last_reset_vol_time",
			"last_reset_req_time",
			"last_rotate_time",
			"used_req",
			"limit_req",
		];
		try {
			const res = await this.pool.query(
				`SELECT column_name FROM information_schema.columns
				  WHERE table_schema = current_schema() AND table_name = 'users'
				    AND data_type = 'integer'
				    AND (column_name = ANY($1) OR column_name = 'port')`,
				[bigintCols],
			);
			for (const row of res.rows) {
				const target = row.column_name === "port" ? "TEXT" : "BIGINT";
				await this.pool.query(`ALTER TABLE users ALTER COLUMN "${row.column_name}" TYPE ${target}`);
			}
			if (res.rows.length > 0) {
				console.log(`[zeus-render] widened users columns: ${res.rows.map((r) => r.column_name).join(", ")}`);
			}
		} catch (e) {
			console.warn(`[zeus-render] schema migration skipped: ${e.message}`);
		}
	}

	async _resolvePk(table) {
		const key = String(table).toLowerCase();
		if (pkCache.has(key)) return pkCache.get(key);
		if (!this.pool) return FALLBACK_PK[key] || null;
		try {
			const res = await this.pool.query(
				`SELECT a.attname AS name
				   FROM pg_index i
				   JOIN pg_class c ON c.oid = i.indrelid
				   JOIN pg_namespace n ON n.oid = c.relnamespace
				   JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
				  WHERE i.indisprimary AND n.nspname = current_schema() AND lower(c.relname) = $1`,
				[key],
			);
			const cols = res.rows.map((r) => r.name);
			const resolved = cols.length > 0 ? cols : FALLBACK_PK[key] || null;
			if (resolved) pkCache.set(key, resolved);
			return resolved;
		} catch (e) {
			const fb = FALLBACK_PK[key] || null;
			if (fb) pkCache.set(key, fb);
			return fb;
		}
	}

	_translate(sql) {
		let s = String(sql);

		// PRAGMA table_info(x) -> information_schema equivalent (row shape kept).
		const pragma = /^\s*PRAGMA\s+table_info\s*\(\s*([A-Za-z_][\w]*)\s*\)\s*;?\s*$/i.exec(s);
		if (pragma) {
			s = {
				__pragmaTableInfo: pragma[1],
				text:
					`SELECT (ordinal_position - 1)::int AS cid, column_name AS name, data_type AS type, ` +
					`(CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END) AS notnull, column_default AS dflt_value, 0 AS pk ` +
					`FROM information_schema.columns WHERE table_schema = current_schema() AND lower(table_name) = lower($1) ` +
					`ORDER BY ordinal_position`,
				params: [pragma[1]],
			};
			return s;
		}

		s = s.replace(/\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b/gi, "SERIAL PRIMARY KEY");
		s = s.replace(/\bAUTOINCREMENT\b/gi, "");

		// SQLite INTEGER is 64-bit and dynamically typed; PostgreSQL int4 is
		// 32-bit and strict. Epoch-ms and high-count columns must be BIGINT,
		// and `port` must accept multi-port strings like "443,2053".
		s = s.replace(
			/\b(last_active|first_connection_time|last_reset_vol_time|last_reset_req_time|last_rotate_time|used_req|limit_req)\s+INTEGER\b/gi,
			"$1 BIGINT",
		);
		s = s.replace(/\bport\s+INTEGER\b/gi, "port TEXT");

		// PG: bare `value` inside DO UPDATE SET is ambiguous (target vs EXCLUDED);
		// SQLite resolved it to the existing row. Qualify with the table name.
		s = s.replace(
			/(INSERT\s+INTO\s+([A-Za-z_][\w]*)[\s\S]*?DO\s+UPDATE\s+SET\s+value\s*=\s*CAST\()value(\s+AS\s+(?:INTEGER|REAL)\s*\))/gi,
			"$1$2.value$3",
		);

		// PG cannot infer the type of a parameter used only in `IS NOT NULL`;
		// SQLite/D1 treated it as text. Cast explicitly to keep semantics.
		s = s.replace(/CASE\s+WHEN\s+\?\s+IS\s+NOT\s+NULL\s+THEN\b/gi, "CASE WHEN CAST(? AS TEXT) IS NOT NULL THEN");

		// Case-insensitive equality against a bound parameter.
		s = s.replace(/([A-Za-z_][\w.]*)\s*=\s*\?\s*COLLATE\s+NOCASE/gi, "lower($1) = lower(?)");

		return s;
	}

	async _finalize(sql, params) {
		const cached = compileCache.get(sql);
		if (cached !== undefined) {
			return { text: cached.text, params: cached.params !== undefined ? cached.params : params };
		}

		let s = this._translate(sql);
		let outParams = params;

		if (s && typeof s === "object" && s.__pragmaTableInfo) {
			const compiled = { text: convertPlaceholders(s.text), params: s.params };
			compileCache.set(sql, compiled);
			return compiled;
		}

		const repl = /^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+([A-Za-z_][\w]*)\s*\(([^)]*)\)(\s*VALUES\s*\([\s\S]*\))\s*;?\s*$/i.exec(
			s,
		);
		if (repl) {
			const table = repl[1];
			const cols = repl[2]
				.split(",")
				.map((c) => c.trim().replace(/^"|"$/g, ""))
				.filter(Boolean);
			const pk = await this._resolvePk(table);
			if (pk && pk.length > 0) {
				const updateCols = cols.filter((c) => !pk.some((p) => p.toLowerCase() === c.toLowerCase()));
				const conflictTarget = `(${pk.join(", ")})`;
				const doUpdate =
					updateCols.length > 0
						? `DO UPDATE SET ${updateCols.map((c) => `${c} = EXCLUDED.${c}`).join(", ")}`
						: "DO NOTHING";
				s = `INSERT INTO ${table} (${cols.join(", ")})${repl[3]} ON CONFLICT ${conflictTarget} ${doUpdate}`;
			}
		}

		outParams = params;
		const compiled = { text: convertPlaceholders(s), params: outParams };
		compileCache.set(sql, { text: compiled.text });
		return { text: compiled.text, params: compiled.params };
	}

	async _query(sql, params) {
		if (!this.pool) throw new Error("Database binding 'DB' unavailable: DATABASE_URL is not configured.");
		const fin = await this._finalize(sql, params || []);
		const res = await this.pool.query(fin.text, fin.params || []);
		return { rows: res.rows || [] };
	}

	async _execute(sql, params) {
		if (!this.pool) throw new Error("Database binding 'DB' unavailable: DATABASE_URL is not configured.");
		const fin = await this._finalize(sql, params || []);
		const res = await this.pool.query(fin.text, fin.params || []);
		return { rowCount: res.rowCount };
	}

	prepare(sql) {
		return new D1PreparedStatement(this, sql);
	}

	async batch(statements) {
		if (!this.pool) throw new Error("Database binding 'DB' unavailable: DATABASE_URL is not configured.");
		const client = await this.pool.connect();
		const results = [];
		try {
			await client.query("BEGIN");
			for (const stmt of statements || []) {
				const fin = await this._finalize(stmt.sqlText, stmt.params || []);
				const res = await client.query(fin.text, fin.params || []);
				results.push({ success: true, meta: { changes: res.rowCount ?? 0 }, results: res.rows });
			}
			await client.query("COMMIT");
		} catch (e) {
			try {
				await client.query("ROLLBACK");
			} catch (e2) { }
			throw e;
		} finally {
			client.release();
		}
		return results;
	}

	async withSession(cb) {
		return cb(this);
	}

	async close() {
		if (this.pool) await this.pool.end();
	}
}

export function createD1FromEnv(env = process.env) {
	return new D1Database(env.DATABASE_URL || "");
}

export default D1Database;
