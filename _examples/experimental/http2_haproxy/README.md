# HAProxy HTTP/2 WebSocket Example

This example demonstrates **protocol conversion** where HAProxy acts as a bridge between:
- **Frontend**: HTTP/1.1 WebSocket connections from clients
- **Backend**: HTTP/2 Extended CONNECT (RFC 8441) to Centrifuge servers

## Architecture

```
┌─────────┐    HTTP/1.1 WS        ┌─────────┐    HTTP/2 (h2)      ┌────────────┐
│ Client1 ├───────────────────────┤         ├─────────────────────┤centrifuge1 │
└─────────┘                       │         │                     └────────────┘
                                  │         │
┌─────────┐    HTTP/1.1 WS        │         │    HTTP/2 (h2)      ┌────────────┐
│ Client2 ├───────────────────────┤ HAProxy ├─────────────────────┤centrifuge2 │
└─────────┘                       │         │                     └────────────┘
                                  │         │
┌─────────┐    HTTP/1.1 WS        │         │    HTTP/2 (h2)      ┌────────────┐
│ Client3 ├───────────────────────┤         ├─────────────────────┤centrifuge3 │
└─────────┘                       └─────────┘                     └────────────┘
```

**Traffic Flow:**
- Multiple client HTTP/1.1 WebSocket connections → HAProxy
- HAProxy multiplexes them into HTTP/2 streams → Backend servers
- Result: Fewer backend connections, better resource utilization

## How It Works

### 1. Client → HAProxy (Frontend)
- Clients connect via traditional HTTP/1.1 WebSocket
- HAProxy accepts on port 443 with `alpn http/1.1`
- Standard `Upgrade: websocket` handshake

### 2. HAProxy → Backend (Protocol Conversion)
- HAProxy converts HTTP/1.1 WebSocket to **HTTP/2 Extended CONNECT** (RFC 8441)
- Uses `:protocol: websocket` pseudo-header
- Negotiates HTTP/2 via ALPN (`alpn h2`)
- Multiple client connections multiplexed into single HTTP/2 connection per backend

### 3. Key Benefits

✅ **Client Compatibility**: Clients use simple HTTP/1.1 WebSocket (maximum compatibility)
✅ **Backend Efficiency**: HTTP/2 multiplexing reduces connection count
✅ **Connection Pooling**: `http-reuse always` enables connection reuse
✅ **Load Balancing**: Round-robin across 3 backend instances
✅ **No HOL Blocking**: Each client still has independent HTTP/1.1 connection

## Requirements

### System Requirements
- Docker and Docker Compose
- `mkcert` for generating locally-trusted certificates

### Install mkcert
```bash
# macOS
brew install mkcert

# Linux
# See https://github.com/FiloSottile/mkcert#installation
```

## Running the Example

### 1. Generate SSL Certificates
```bash
cd _examples/experimental/http2_haproxy
./generate-certs.sh
```

This creates locally-trusted certificates using `mkcert` (no browser warnings!).

### 2. Build the Server Binary
```bash
./build.sh
```

This builds the server binary for Linux (for Docker containers).

**Important:** Certificates must be generated BEFORE building Docker images, as they are baked into the container images and the mkcert CA is installed in the system trust store.

### 3. Start the Stack
```bash
docker-compose up --build
```

This starts:
- **HAProxy** (port 443) - Frontend load balancer
- **centrifuge1, centrifuge2, centrifuge3** - Backend servers
- **HAProxy Stats** (port 8404) - Monitoring dashboard

### 3. Open the Demo
Open in your browser:
- **Demo Application**: https://localhost
- **HAProxy Stats**: http://localhost:8404

### 4. Verify Protocol Conversion

#### In Chrome DevTools:
1. Open DevTools (F12) → **Network** tab
2. Filter by **WS** (WebSocket)
3. Click on the WebSocket connection
4. Check **Headers** tab → Look for `Upgrade: websocket` (HTTP/1.1 from client perspective)

#### In HAProxy Stats (http://localhost:8404):
- View active connections to backends
- See HTTP/2 session reuse
- Monitor connection pooling efficiency

#### Check Backend Logs:
```bash
docker-compose logs centrifuge1 | grep "protocol:"
```

You should see:
```
[centrifuge1] user 42 connected via websocket with protocol: json
```

The backend receives the connection via HTTP/2, but Centrifuge sees it as a normal WebSocket.

## How to Monitor

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f haproxy
docker-compose logs -f centrifuge1

# Filter for connections
docker-compose logs | grep "connected via"
```

### HAProxy Stats Dashboard
Visit http://localhost:8404 to see:
- Active sessions per backend
- HTTP/2 connection reuse metrics
- Health check status
- Request/response rates

### Count Connections
```bash
# On macOS - count connections to HAProxy
netstat -an | grep "\.443.*ESTABLISHED" | wc -l

# Inside HAProxy container
docker-compose exec haproxy netstat -an | grep ESTABLISHED
```

## Configuration Details

### HAProxy Configuration (`haproxy/haproxy.cfg`)

**Frontend** (accepts HTTP/1.1):
```
bind *:443 ssl crt /certs/haproxy.pem alpn http/1.1
```

**Backend** (converts to HTTP/2):
```
http-reuse always  # Critical for HTTP/2 multiplexing
server centrifuge1 centrifuge1:8443 ssl alpn h2 verify none
```

The magic happens automatically - HAProxy 2.4+ implements RFC 8441 and converts HTTP/1.1 WebSocket upgrade requests to HTTP/2 Extended CONNECT.

### Backend Server Configuration

The backend servers run with:
```bash
GODEBUG=http2xconnect=1
```

This enables Go's HTTP/2 Extended CONNECT support (required for WebSocket over HTTP/2).

## Testing Scenarios

### 1. Test Multiple Clients
Open multiple browser tabs to https://localhost and watch:
- HAProxy load balance across backends
- Each client connection handled independently
- Instance names shown in messages

### 2. Test Connection Persistence
Send messages from different tabs:
- Notice which backend instance handles each client
- HAProxy maintains session affinity for WebSocket duration
- Messages from all instances appear in all tabs (pub/sub)

### 3. Test Backend Scaling
```bash
# Stop one backend
docker-compose stop centrifuge1

# HAProxy automatically redistributes to remaining backends
# Refresh browser - should still work

# Restart
docker-compose start centrifuge1
```

## Important Notes

### HAProxy Version Requirements
- **HAProxy 2.4+** required for WebSocket over HTTP/2 backend support (RFC 8441)
- This example uses HAProxy 2.9

### Backend Requirements
Your Centrifuge/Centrifugo backend must:
- Support HTTP/2 (Go's `net/http` with TLS does this automatically)
- Enable Extended CONNECT via `GODEBUG=http2xconnect=1`
- Handle `:protocol: websocket` pseudo-header in CONNECT requests

### Differences from Direct HTTP/2

In the `/experimental/http2` example:
- Clients connect **directly** via HTTP/2 WebSocket
- Requires browser support for HTTP/2 WebSocket (Chrome, Firefox, Safari)

In **this** example:
- Clients connect via HTTP/1.1 (traditional WebSocket)
- HAProxy converts to HTTP/2 on backend
- **Better client compatibility** + **backend efficiency**

## Performance Benefits

### Connection Reduction
- **Without HAProxy**: 100 clients = 100 backend connections
- **With HAProxy + HTTP/2**: 100 clients → ~3-10 backend connections (depending on load balancing)

### Resource Savings
- Fewer TCP handshakes to backends
- Reduced memory footprint on backends
- Better CPU cache utilization
- HTTP/2 header compression (HPACK)

### When to Use This Pattern
✅ Large number of concurrent clients
✅ Backend connection limits
✅ Need HTTP/1.1 client compatibility
✅ Want to reduce backend connection overhead
✅ Using load balancing anyway

## Troubleshooting

### Browser Shows Certificate Warning
Run `./generate-certs.sh` again to ensure `mkcert -install` succeeded.

### Connection Refused
Check if all services are running:
```bash
docker-compose ps
```

### Backend Not Receiving HTTP/2
Check backend logs for protocol version:
```bash
docker-compose logs centrifuge1 | grep "connected via"
```

Should show connections being accepted.

### HAProxy Not Converting to HTTP/2
Check HAProxy logs:
```bash
docker-compose logs haproxy
```

Ensure `alpn h2` is in backend server configuration.

## Files in This Example

- `docker-compose.yml` - Container orchestration
- `haproxy/haproxy.cfg` - HAProxy configuration
- `Dockerfile` - Minimal container image (copies pre-built binary)
- `main.go` - Centrifuge server code
- `index.html` - Demo web client
- `build.sh` - Build script for server binary
- `generate-certs.sh` - Certificate generation script
- `README.md` - This file

## References

- [RFC 8441 - Bootstrapping WebSockets with HTTP/2](https://datatracker.ietf.org/doc/html/rfc8441)
- [HAProxy WebSocket Documentation](https://www.haproxy.com/blog/websockets-load-balancing-with-haproxy)
- [Go HTTP/2 Extended CONNECT](https://github.com/golang/go/issues/53208)
- [Centrifuge Documentation](https://centrifugal.dev/)

## Clean Up

```bash
# Stop and remove containers
docker-compose down

# Also remove volumes
docker-compose down -v

# Remove generated files
rm -rf certs/
rm -f server
```
