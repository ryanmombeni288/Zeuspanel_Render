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
try {
	pg.types.setTypeParser(1114, (v) => v);
} catch (e) { }

const FALLBACK_PK = { settings: ["key"] };
const pkCache = new Map();

function coerceParam(v) {
	if (v === undefined) return null;
	if (v instanceof ArrayBuffer) return Buffer.from(v);
	if (ArrayBuffer.isView(v)) return Buffer.from(v.buffer, v.byteOffset, v.byteLength);
	return v;
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

	_pkColumns(table) {
		const key = String(table).toLowerCase();
		if (pkCache.has(key)) return pkCache.get(key);
		const cached = FALLBACK_PK[key];
		if (cached) {
			pkCache.set(key, cached);
			return cached;
		}
		// Discover lazily; will be cached by the async wrapper below.
		return null;
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

		// Case-insensitive equality against a bound parameter.
		s = s.replace(/([A-Za-z_][\w.]*)\s*=\s*\?\s*COLLATE\s+NOCASE/gi, "lower($1) = lower(?)");

		return s;
	}

	async _finalize(sql, params) {
		let s = this._translate(sql);
		let outParams = params;

		if (s && typeof s === "object" && s.__pragmaTableInfo) {
			return s;
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
		return { text: s, params: outParams };
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
