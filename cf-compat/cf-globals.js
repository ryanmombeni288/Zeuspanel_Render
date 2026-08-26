// Installs Cloudflare-Workers-shaped globals needed by Source.js on Node.js:
//   - caches.default            (in-memory TTL cache shim)
//   - WebSocketPair             (ws-backed pair shim)
//   - Response                  (spec-compliant Node Responses throw on status
//                                101; Workers allow it for WebSockets, so we
//                                wrap the constructor to support exactly that)
//   - WebSocket                 (statics fallback for runtimes without one;
//                                Node >= 22 ships a real global WebSocket)
import caches from "./caches-shim.js";
import WebSocketPair from "./websocket-pair.js";

export function installCFCompatGlobals() {
	const g = globalThis;

	if (!g.caches) {
		g.caches = caches;
	}

	g.WebSocketPair = WebSocketPair;

	if (typeof g.WebSocket === "undefined") {
		class WebSocketStub {}
		WebSocketStub.CONNECTING = 0;
		WebSocketStub.OPEN = 1;
		WebSocketStub.CLOSING = 2;
		WebSocketStub.CLOSED = 3;
		g.WebSocket = WebSocketStub;
	}

	if (!g.__ZEUS_RESPONSE_PATCHED__) {
		const RealResponse = g.Response;
		const RealHeaders = g.Headers;

		function ZeusResponse(body, init) {
			const status = init && typeof init === "object" && typeof init.status === "number" ? init.status : null;
			if (status !== null && (status < 200 || status > 599)) {
				// Workers-style non-HTTP-status response (101 Switching Protocols).
				let headers;
				try {
					headers = new RealHeaders((init && init.headers) || {});
				} catch (e) {
					headers = new RealHeaders();
				}
				return {
					status,
					statusText: (init && init.statusText) || "",
					ok: false,
					headers,
					body: null,
					bodyUsed: false,
					url: "",
					type: "default",
					redirected: false,
					webSocket: (init && init.webSocket) || null,
					async arrayBuffer() {
						throw new Error("not supported on switching-protocol response");
					},
					async blob() {
						throw new Error("not supported on switching-protocol response");
					},
					async formData() {
						throw new Error("not supported on switching-protocol response");
					},
					json() {
						throw new Error("not supported on switching-protocol response");
					},
					text() {
						return Promise.resolve("");
					},
					clone() {
						throw new Error("cannot clone switching-protocol response");
					},
				};
			}
			return new RealResponse(body, init);
		}
		ZeusResponse.prototype = RealResponse.prototype;
		ZeusResponse.error = RealResponse.error.bind(RealResponse);
		ZeusResponse.redirect = RealResponse.redirect.bind(RealResponse);
		if (RealResponse.json) ZeusResponse.json = RealResponse.json.bind(RealResponse);
		Object.defineProperty(ZeusResponse, "name", { value: "Response" });

		g.Response = ZeusResponse;
		g.__ZEUS_RESPONSE_PATCHED__ = true;
	}
}

export default installCFCompatGlobals;
