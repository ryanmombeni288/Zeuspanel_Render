// Cloudflare `WebSocketPair` + Workers WebSocket emulation for Node.js,
// backed by the `ws` package at the transport boundary (wired up in server.js).
//
// Source.js uses the exact Workers semantics:
//   const pair = new WebSocketPair();
//   const [clientSock, serverSock] = Object.values(pair);
//   serverSock.accept();
//   serverSock.addEventListener("message"|"close"|"error", ...)
//   serverSock.send(uint8array | arraybuffer)
//   serverSock.close(code, reason)
//   return new Response(null, { status: 101, webSocket: clientSock });
//
// The HTTP adapter runs the worker fetch inside an AsyncLocalStorage scope;
// the pair registers its server side there so the adapter can bind it to the
// real network socket without touching Source.js.
import { AsyncLocalStorage } from "node:async_hooks";

export const wsRequestContext = new AsyncLocalStorage();

const CONNECTING = 0;
const OPEN = 1;
const CLOSING = 2;
const CLOSED = 3;

function toArrayBuffer(buf) {
	return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
}

function toBuffer(data) {
	if (Buffer.isBuffer(data)) return data;
	if (ArrayBuffer.isView(data)) return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
	if (data instanceof ArrayBuffer) return Buffer.from(data);
	return Buffer.from(String(data));
}

class ZeusCompatWebSocket {
	constructor() {
		this._listeners = { message: [], close: [], error: [], open: [] };
		this.readyState = CONNECTING;
		this.binaryType = "blob"; // Source.js sets "arraybuffer" on the server side
		this._peer = null;
		this._transport = null;
		this._pendingBytes = 0;
		this._closeCode = null;
		this._pendingClose = null; // close requested before transport bind (Workers queue it)
	}
	accept() {
		if (this.readyState === CONNECTING) this.readyState = OPEN;
	}
	addEventListener(type, fn) {
		const list = this._listeners[type];
		if (list && !list.includes(fn)) list.push(fn);
	}
	removeEventListener(type, fn) {
		const list = this._listeners[type];
		if (!list) return;
		const i = list.indexOf(fn);
		if (i !== -1) list.splice(i, 1);
	}
	get bufferedAmount() {
		if (this._transport && typeof this._transport.bufferedAmount === "number") {
			return this._transport.bufferedAmount;
		}
		return this._pendingBytes;
	}
	send(data) {
		if (this.readyState !== OPEN) throw new Error("WebSocket is not open");
		const payload = toBuffer(data);
		if (this._transport) {
			this._transport.send(payload, { binary: typeof data !== "string" });
			return;
		}
		if (this._peer) {
			this._peer._receive(typeof data === "string" ? data : payload, typeof data !== "string");
			return;
		}
		throw new Error("WebSocket has no bound transport");
	}
	close(code, reason) {
		if (this.readyState === CLOSING || this.readyState === CLOSED) return;
		this.readyState = CLOSING;
		const c = typeof code === "number" ? code : 1000;
		const r = reason || "";
		if (this._transport) {
			try {
				this._transport.close(c, r);
			} catch (e) { }
		}
		// If the transport is not attached yet (adapter binds it after fetch
		// returns), remember the close so it is applied on bind.
		this._pendingClose = { code: c, reason: r };
		if (this._peer) {
			this._peer._onPeerClose(c, r);
		}
		this._finalizeClose(c, r);
	}
	// --- internal plumbing -------------------------------------------------
	_receive(data, isBinary) {
		if (this.readyState !== OPEN) return;
		let out = data;
		if (isBinary || Buffer.isBuffer(data)) {
			const buf = toBuffer(data);
			out =
				this.binaryType === "arraybuffer"
					? toArrayBuffer(buf)
					: this.binaryType === "uint8array"
						? new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength)
						: toArrayBuffer(buf); // default: hand over ArrayBuffer like Workers do
		}
		this._emit("message", { data: out, type: isBinary ? "binary" : "text", lastEventId: "" });
	}
	_emit(type, event) {
		const list = this._listeners[type];
		if (!list) return;
		for (const fn of list.slice()) {
			try {
				fn(event);
			} catch (e) { }
		}
	}
	_onPeerClose(code, reason) {
		if (this.readyState === CLOSED) return;
		this.readyState = CLOSED;
		this._emit("close", { code: code ?? 1005, reason: reason || "", wasClean: true });
	}
	_finalizeClose(code, reason) {
		this.readyState = CLOSED;
		this._emit("close", { code: code ?? 1005, reason: reason || "", wasClean: true });
		if (this._peer && this._peer.readyState !== CLOSED) {
			this._peer._onPeerClose(code ?? 1000, reason || "");
		}
	}
	_bindTransport(ws) {
		this._transport = ws;
		if (this.readyState === CONNECTING) this.readyState = OPEN;
		ws.on("message", (data, isBinaryFlag) => {
			const d = Array.isArray(data) ? data[0] : data;
			const bin = typeof isBinaryFlag === "boolean" ? isBinaryFlag : !Buffer.isBuffer(d);
			this._receive(d, bin);
		});
		ws.on("close", (code, reason) => {
			if (this.readyState === CLOSED) return; // already closed app-side
			this.readyState = CLOSED;
			this._emit("close", { code: code || 1006, reason: reason ? reason.toString() : "", wasClean: true });
		});
		ws.on("error", (err) => {
			this._emit("error", { error: err, message: err && err.message });
		});
		// Replay a close() that happened before the transport was attached.
		if (this._pendingClose && this.readyState !== OPEN) {
			const pc = this._pendingClose;
			try {
				ws.close(pc.code, pc.reason);
			} catch (e) { }
		}
	}
	_onTransportSend(payload, opts) {
		if (this._transport) this._transport.send(payload, opts);
		else if (this._peer) this._peer._receive(payload, true);
	}
}

class WebSocketPair {
	constructor() {
		const client = new ZeusCompatWebSocket();
		const server = new ZeusCompatWebSocket();
		client._peer = server;
		server._peer = client;
		this[0] = client; // runtime side (returned in the Response)
		this[1] = server; // application side
		const store = wsRequestContext.getStore();
		if (store) store.pair = server;
	}
}

export { ZeusCompatWebSocket, WebSocketPair };
export default WebSocketPair;
