# HTTP/2 WebSocket Example

This example demonstrates WebSocket over HTTP/2 using RFC 8441 extended CONNECT protocol.

## Running the Example

First gen certs following certs/README.md instructions.

Then run the server with:

```bash
GODEBUG=http2xconnect=1 go run main.go
```

Open https://localhost:8080 in your browser.

## Important Notes

1. **GODEBUG environment variable**: The Go standard library requires `GODEBUG=http2xconnect=1` to enable HTTP/2 extended CONNECT support. See https://github.com/golang/go/issues/53208

2. **Browser Support**: Modern browsers (Chrome, Firefox, Safari) will automatically negotiate HTTP/2 when connecting via `wss://` (secure WebSocket). The browser handles the HTTP/2 upgrade transparently.

3. **Network Inspection**: To verify HTTP/2 is being used:
   - Open browser DevTools → Network tab
   - WebSocket connections over HTTP/2 will have "200" status code instead of 101.

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
centrifuge.WebsocketConfig{
    EnableHTTP2ExtendedConnect: true,
}
```

This allows the same endpoint to accept:
- HTTP/1.1 WebSocket upgrade (traditional `Upgrade: websocket`)
- HTTP/2 extended CONNECT (RFC 8441 with `:protocol: websocket` pseudo-header)

The client (browser) automatically chooses the appropriate protocol based on the connection type.

## Emulate Latency

# Create pipes with 50ms delay each way (100ms RTT), apply to port 8080:

```
sudo dnctl pipe 1 config delay 50
sudo dnctl pipe 2 config delay 50
sudo pfctl -e
echo "
dummynet in proto tcp from any port 8080 to any pipe 1
dummynet out proto tcp from any to any port 8080 pipe 2
" | sudo pfctl -f -
```

Clean up:

```
sudo dnctl -q flush
sudo pfctl -d
```
