// Cloudflare `cloudflare:sockets` -> Node.js `node:net` compatibility shim.
//
// Emulates the subset of the Cloudflare TCP socket API used by Source.js:
//   const socket = connect({ hostname, port });
//   socket.opened            -> Promise<void>
//   socket.readable          -> ReadableStream<Uint8Array>
//   socket.writable          -> WritableStream<Uint8Array>
//   socket.close()           -> void
//
import net from "node:net";

const OPENING = 0;
const OPEN = 1;
const CLOSING = 2;
const CLOSED = 3;

export function connect(address, optionsOrInit = {}) {
	// Support both CF signatures: connect({hostname, port}) and connect(addr, {port})
	let hostname;
	let port;
	if (typeof address === "object" && address !== null) {
		hostname = address.hostname;
		port = address.port;
	} else {
		hostname = address;
		port = typeof optionsOrInit === "object" && optionsOrInit !== null ? optionsOrInit.port : optionsOrInit;
	}

	let sock;
	if (typeof hostname === "string" && net.isIP(hostname.replace(/^\[|\]$/g, "")) === 6) {
		sock = net.connect({ host: hostname.replace(/^\[|\]$/g, ""), port: Number(port), family: 6 });
	} else {
		sock = net.connect({ host: hostname, port: Number(port) });
	}

	// Runtime-difference safeguard only: keep dead upstream connections from
	// lingering forever on a persistent Node.js process (Workers reaped sockets
	// automatically when the request context ended).
	try {
		sock.setKeepAlive(true, 15000);
		sock.setNoDelay(true);
	} catch (e) { }

	const state = { readyState: OPENING };
	let openedResolve;
	let openedReject;
	const opened = new Promise((resolve, reject) => {
		openedResolve = resolve;
		openedReject = reject;
	});

	let readableController = null;
	let readableClosed = false;
	let writableController = null;
	let writeErr = null;

	const readable = new ReadableStream({
		start(controller) {
			readableController = controller;
		},
		cancel() {
			readableClosed = true;
			try { sock.destroy(); } catch (e) { }
		},
	});

	const writable = new WritableStream({
		start(controller) {
			writableController = controller;
		},
		write(chunk) {
			return new Promise((resolve, reject) => {
				const buf = ArrayBuffer.isView(chunk)
					? Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength)
					: Buffer.from(chunk);
				if (!sock.writable) {
					reject(writeErr || new Error("socket is not writable"));
					return;
				}
				sock.write(buf, (err) => (err ? reject(err) : resolve()));
			});
		},
		abort() {
			try { sock.destroy(); } catch (e) { }
		},
		close() {
			return new Promise((resolve) => {
				if (!sock.writable) return resolve();
				sock.end(() => resolve());
			});
		},
	});

	sock.on("connect", () => {
		state.readyState = OPEN;
		if (openedResolve) openedResolve();
	});
	sock.on("data", (buf) => {
		if (readableController && !readableClosed) {
			try {
				readableController.enqueue(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength));
			} catch (e) { }
		}
	});
	const fail = (err) => {
		state.readyState = CLOSED;
		if (openedReject) openedReject(err || new Error("connection failed"));
		openedReject = null;
		if (writableController && err) {
			try { writableController.error(err); } catch (e) { }
			writableController = null;
		}
		try {
			readableController?.close();
		} catch (e) { }
	};
	sock.on("error", fail);
	sock.on("end", () => {
		// Peer half-closed; finish our side too unless writes are still pending.
		if (sock.writableEnded) return;
		try { sock.end(); } catch (e) { }
	});
	sock.on("close", () => {
		state.readyState = CLOSED;
		if (openedReject) openedReject(new Error("connection closed before established"));
		openedReject = null;
		try {
			readableController?.close();
		} catch (e) { }
	});

	return {
		get readyState() {
			return state.readyState;
		},
		opened,
		closed: new Promise((resolve) => {
			sock.once("close", () => resolve());
		}),
		readable,
		writable,
		close() {
			state.readyState = CLOSING;
			try {
				sock.destroy();
			} catch (e) { }
		},
	};
}

export default connect;
