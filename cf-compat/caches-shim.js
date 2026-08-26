// Minimal Cloudflare `caches.default` emulation for Node.js.
// Used by Source.js only as a TTL-based throttle for internal pseudo-URLs
// ("https://internal.zeus/*"), so an in-memory Map with Cache-Control
// max-age parsing is behaviorally equivalent for a single-instance service.

function parseMaxAge(cacheControl) {
	if (!cacheControl) return 0;
	const m = /max-age\s*=\s*(\d+)/i.exec(cacheControl);
	return m ? parseInt(m[1], 10) || 0 : 0;
}

class DefaultCache {
	constructor() {
		this._store = new Map();
	}
	_key(request) {
		try {
			return typeof request === "string" ? request : request.url;
		} catch (e) {
			return String(request);
		}
	}
	async match(request) {
		const key = this._key(request);
		const entry = this._store.get(key);
		if (!entry) return undefined;
		if (Date.now() > entry.expires) {
			this._store.delete(key);
			return undefined;
		}
		return new Response("1", { headers: { "Content-Type": "text/plain" } });
	}
	async put(request, response) {
		const key = this._key(request);
		let ttl = 3600;
		try {
			ttl = parseMaxAge(response.headers.get("Cache-Control"));
		} catch (e) { }
		if (ttl <= 0) return;
		if (this._store.size > 1024) {
			const oldest = this._store.keys().next().value;
			if (oldest !== undefined) this._store.delete(oldest);
		}
		this._store.set(key, { expires: Date.now() + ttl * 1000 });
	}
	async delete(request) {
		return this._store.delete(this._key(request));
	}
}

const caches = {
	default: new DefaultCache(),
};

export default caches;
