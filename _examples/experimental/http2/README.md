# HTTP/2 WebSocket Example

This example demonstrates WebSocket over HTTP/2 using RFC 8441 extended CONNECT protocol.

The server supports both:
- **HTTP/2 cleartext (h2c)** - for local development
- **HTTP/2 over TLS** - for production use

## Running the Example

### Option 1: H2C (HTTP/2 Cleartext) - Default

```bash
cd _examples/experimental/http2
GODEBUG=http2xconnect=1 go run main.go
```

Open https://localhost:8443 in your browser (you may need to accept the self-signed certificate warning).

## Important Notes

1. **GODEBUG environment variable**: The Go standard library requires `GODEBUG=http2xconnect=1` to enable HTTP/2 extended CONNECT support. See https://github.com/golang/go/issues/53208

2. **Browser Support**: Modern browsers (Chrome, Firefox, Safari) will automatically negotiate HTTP/2 when connecting via `wss://` (secure WebSocket). The browser handles the HTTP/2 upgrade transparently.

3. **Network Inspection**: To verify HTTP/2 is being used:
   - Open browser DevTools → Network tab
   - Look for the "Protocol" column (you may need to enable it)
   - WebSocket connections over HTTP/2 will show "h2" as the protocol

## Server Options

```
-port int
    Port to bind app to (default 8080)
-cert string
    TLS certificate file (default "certs/localhost.pem")
-key string
    TLS key file (default "certs/localhost-key.pem")
```

## How It Works

The server configures the WebSocket upgrader with:

```go
upgrader := websocket.Upgrader{
    Protocol: websocket.ProtocolAcceptAny, // Accept both HTTP/1.1 and HTTP/2
}
```

This allows the same endpoint to accept:
- HTTP/1.1 WebSocket upgrade (traditional `Upgrade: websocket`)
- HTTP/2 extended CONNECT (RFC 8441 with `:protocol: websocket` pseudo-header)

The client (browser) automatically chooses the appropriate protocol based on the connection type.
