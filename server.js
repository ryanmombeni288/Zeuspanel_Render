// Railway.com / Render.com / Node.js entry point for the ZEUS panel (Cloudflare Workers port).
//
// Bridges Node's http server to the original Worker fetch handler with the
// smallest possible surface change:
//   - listens on process.env.PORT bound to 0.0.0.0
//   - converts Node req/res <-> Fetch API Request/Response
//   - injects CF-Connecting-IP (Render sits behind a proxy; Workers had it built in)
//   - provides env bindings: DB (PostgreSQL-backed D1 emulation), CF_API_TOKEN,
//     CF_ACCOUNT_ID, WORKER_NAME
//   - provides ctx.waitUntil()
//   - terminates WebSocket upgrades on the same port via `ws`, binding them to
//     the WebSocketPair created inside the handler
//   - adds /healthz for Render health checks (does not interfere with routes)
import http from "node:http";
import zlib from "node:zlib";
import { Readable } from "node:stream";

import { installCFCompatGlobals } from "./cf-compat/cf-globals.js";
installCFCompatGlobals();

import worker from "./Source.js";
import { D1Database } from "./cf-compat/d1.js";
import WebSocketPairShim, { wsRequestContext } from "./cf-compat/websocket-pair.js";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 3000);
const HOST = "0.0.0.0";

const db = new D1Database(
	process.env.DATABASE_URL ||
	process.env.DATABASE_PRIVATE_URL ||
	process.env.DATABASE_PUBLIC_URL ||
	""
);
const env = {
	DB: db,
	CF_API_TOKEN: process.env.CF_API_TOKEN || "",
	CF_ACCOUNT_ID: process.env.CF_ACCOUNT_ID || "",
	WORKER_NAME: process.env.WORKER_NAME || "",
};

function makeCtx() {
	const pending = [];
	return {
		__pending: pending,
		waitUntil(promise) {
			pending.push(Promise.resolve(promise).catch(() => { }));
		},
		passThroughOnException() { },
	};
}

function settleCtx(ctx) {
	if (!ctx || !ctx.__pending || ctx.__pending.length === 0) return;
	const list = ctx.__pending.splice(0);
	Promise.allSettled(list).catch(() => { });
}

function buildRequest(req) {
	let proto = req.headers["x-forwarded-proto"] || (req.socket && req.socket.encrypted ? "https" : "http");
	if (Array.isArray(proto)) proto = proto[0];
	proto = String(proto).split(",")[0].trim();
	const rawHost = Array.isArray(req.headers.host) ? req.headers.host[0] : req.headers.host;
	const host = rawHost || `${req.socket?.remoteAddress || "localhost"}:${req.socket?.remotePort || PORT}`;
	const url = `${proto}://${host}${req.url}`;

	const headers = new Headers();
	for (const [name, value] of Object.entries(req.headers)) {
		if (value === undefined) continue;
		if (Array.isArray(value)) value.forEach((v) => headers.append(name, v));
		else headers.set(name, value);
	}

	// Workers always exposed the client IP via CF-Connecting-IP; on Render the
	// service sits behind Render's proxy, so derive it once here.
	try {
		let ip = headers.get("CF-Connecting-IP");
		if (!ip) {
			const xff = req.headers["x-forwarded-for"];
			ip = (Array.isArray(xff) ? xff[0] : xff) || "";
			if (ip.includes(",")) ip = ip.split(",")[0].trim();
			ip = ip.trim();
			if (!ip && req.socket && req.socket.remoteAddress) ip = req.socket.remoteAddress;
			if (ip.startsWith("::ffff:")) ip = ip.slice(7);
			if (ip) headers.set("CF-Connecting-IP", ip);
		}
	} catch (e) { }

	const method = String(req.method || "GET").toUpperCase();
	const hasBody = method !== "GET" && method !== "HEAD" && method !== "OPTIONS";
	const init = { method, headers };
	if (hasBody) {
		init.body = Readable.toWeb(req);
		init.duplex = "half";
	}
	return new Request(url, init);
}

async function sendResponse(nodeReq, nodeRes, response) {
	if (!response || response.__isZeusCompat101 === true || response.status === 101) return;

	const headers = {};
	response.headers.forEach((value, name) => {
		if (name.toLowerCase() === "set-cookie") return;
		headers[name] = value;
	});
	try {
		if (typeof response.headers.getSetCookie === "function") {
			const sc = response.headers.getSetCookie();
			if (sc && sc.length > 0) headers["Set-Cookie"] = sc;
		}
	} catch (e) { }

	if (!response.body) {
		nodeRes.writeHead(response.status, headers);
		nodeRes.end();
		return;
	}

	const stream = Readable.fromWeb(response.body);
	stream.on("error", () => {
		try { nodeRes.destroy(); } catch (e) { }
	});

	// Compress text payloads (panel HTML is ~300KB uncompressed; JSON APIs are
	// polled continuously). Skipped when the client sends no Accept-Encoding
	// or the worker already encoded the body.
	const accept = String((nodeReq && nodeReq.headers && nodeReq.headers["accept-encoding"]) || "");
	const contentType = String(headers["content-type"] || "");
	const canGzip =
		/\bgzip\b/.test(accept) &&
		headers["content-encoding"] === undefined &&
		(contentType.startsWith("text/") ||
			contentType.includes("json") ||
			contentType.includes("javascript") ||
			contentType.includes("svg") ||
			contentType.includes("manifest"));

	if (canGzip) {
		const outHeaders = { ...headers };
		outHeaders["Content-Encoding"] = "gzip";
		outHeaders["Vary"] = headers["vary"] ? `${headers["vary"]}, Accept-Encoding` : "Accept-Encoding";
		delete outHeaders["content-length"];
		nodeRes.writeHead(response.status, outHeaders);
		const gzip = zlib.createGzip();
		gzip.on("error", () => {
			try { nodeRes.destroy(); } catch (e) { }
		});
		stream.pipe(gzip).pipe(nodeRes);
		return;
	}

	nodeRes.writeHead(response.status, headers);
	stream.pipe(nodeRes);
}

const wss = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 * 1024 });

const server = http.createServer(async (req, res) => {
	let ctx = null;
	try {
		const pathname = (req.url || "/").split("?")[0];
		if (pathname === "/healthz") {
			res.writeHead(200, { "Content-Type": "application/json" });
			res.end(JSON.stringify({ status: "ok" }));
			return;
		}
		const request = buildRequest(req);
		ctx = makeCtx();
		const response = await wsRequestContext.run({ pair: null }, () => worker.fetch(request, env, ctx));
		await sendResponse(req, res, response);
	} catch (err) {
		console.error("[zeus-panel] request error:", (err && err.stack) || err);
		try {
			if (!res.headersSent) {
				res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
			}
			res.end("Internal Server Error");
		} catch (e) { }
	} finally {
		settleCtx(ctx);
	}
});

server.on("upgrade", async (req, socket, head) => {
	let ctx = null;
	try {
		wss.handleUpgrade(req, socket, head, async (ws) => {
			try {
				const request = buildRequest(req);
				ctx = makeCtx();
				const store = { pair: null };
				const response = await wsRequestContext.run(store, () => worker.fetch(request, env, ctx));
				const serverSide = store.pair || (response && response.webSocket);
				if (serverSide && typeof serverSide._bindTransport === "function") {
					serverSide._bindTransport(ws); // handshake already completed by handleUpgrade
				} else {
					try { ws.close(1011, "no websocket pair"); } catch (e) { }
				}
			} catch (err) {
				console.error("[zeus-panel] websocket error:", (err && err.stack) || err);
				try { ws.close(1011, "internal error"); } catch (e) { }
			} finally {
				settleCtx(ctx);
			}
		});
	} catch (e) {
		try { socket.destroy(); } catch (e2) { }
	}
});

if (!db.connected) {
	console.warn(
		"[zeus-panel] WARNING: DATABASE_URL (or DATABASE_PRIVATE_URL) is not set. The app will start, but database-backed routes will fail until you configure it (see .env.example).",
	);
}

db.ping()
	.then(async () => {
		console.log("[zeus-panel] PostgreSQL connection OK");
		await db.migrate();
	})
	.catch((e) => {
		if (db.connected) {
			console.warn(`[zeus-panel] PostgreSQL not reachable yet: ${e.message}`);
			console.warn("[zeus-panel] Will keep retrying lazily per-request (same behavior as D1 error paths).");
		}
	});

// Railway & Render proxies close idle connections; keep-alive headers avoid 502 races.
server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;
server.requestTimeout = 0;

process.on("unhandledRejection", (err) => {
	console.error("[zeus-panel] unhandled rejection:", err && err.stack ? err.stack : err);
});

server.listen(PORT, HOST, () => {
	console.log(`[zeus-panel] ZEUS Panel listening on http://${HOST}:${PORT}`);
});

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

let shuttingDown = false;
function gracefulShutdown(signal) {
	if (shuttingDown) return;
	shuttingDown = true;
	console.log(`[zeus-panel] Received ${signal}, shutting down...`);
	server.close(() => { });
	wss.clients.forEach((c) => {
		try { c.close(1001, "server shutting down"); } catch (e) { }
	});
	setTimeout(() => process.exit(0), 5000).unref();
	db.close()
		.then(() => process.exit(0))
		.catch(() => process.exit(0));
}

export { server };
