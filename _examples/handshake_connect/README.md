Example demonstrates handshake connect: the client sends its connect and subscribe commands inside the WebSocket handshake request, and the server replies right after the handshake. The connection is ready one round trip earlier.

The page compares three scenarios side by side:

* **Standard connect** – connect and subscribe are sent after the WebSocket opens: two round trips.
* **Handshake connect** – the same commands travel inside the handshake request: one round trip.
* **Behind a strict proxy** – a simulated proxy rejects the larger handshake (like nginx does for header lines above `large_client_header_buffers`). The client retries without the data, connects as usual, and keeps going without it on reconnect.

To make the round trip visible on localhost, the example starts a small proxy in front of the server which adds latency (100ms round trip by default).

The client needs centrifuge-js with the `maxHandshakeConnectSize` option. Until it's released, run its dev server from the centrifuge-js repo, which serves the build at http://localhost:2000/centrifuge.js:

```
npm run dev
```

Then from this directory:

```
go run main.go
```

Use `-centrifuge_js` to load the build from another URL.

And open http://localhost:8000. Flags: `-rtt` to change the added round trip time, `-proxy_header_limit` to change the size the strict proxy accepts.

How it works:

* Server: `WebsocketConfig.HandshakeConnect: true`.
* Client: `maxHandshakeConnectSize: 4096` – the maximum size of encoded commands to send inside the handshake. The data goes in the `Sec-WebSocket-Protocol` request header as `cf-connect.<base64url>`, offered after `cf-json-hc` and `centrifuge-json`. A server which takes the commands selects `cf-json-hc`; otherwise it selects `centrifuge-json` and the client sends the commands after the connection opens. The data is never echoed back. Commands which don't fit the size go after open too.
