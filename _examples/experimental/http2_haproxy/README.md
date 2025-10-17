# HAProxy HTTP/2 WebSocket Example

This example demonstrates **protocol conversion** where HAProxy acts as a bridge between:
- **Frontend**: HTTP/1.1 or HTTP/2 WebSocket connections from clients
- **Backend**: HTTP/2 Extended CONNECT (RFC 8441) to Centrifuge servers

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
4. Check status code - must be `101 Switching Protocols` for HTTP/1.1 and `200 OK` for HTTP/2.

#### In HAProxy Stats (http://localhost:8404):
- View active connections to backends
- See HTTP/2 session reuse
- Monitor connection pooling efficiency

### HAProxy Stats Dashboard
Visit http://localhost:8404

### Count Connections
```bash
# On macOS - count connections to HAProxy
netstat -an | grep "\.443.*ESTABLISHED" | wc -l

# Inside HAProxy container
docker-compose exec haproxy netstat -an | grep ESTABLISHED
```

The backend servers run with:
```bash
GODEBUG=http2xconnect=1
```

This enables Go's HTTP/2 Extended CONNECT support (required for WebSocket over HTTP/2).

### Test Backend Scaling
```bash
# Stop one backend
docker-compose stop centrifuge1

# HAProxy automatically redistributes to remaining backends
# Refresh browser - should still work

# Restart
docker-compose start centrifuge1
```

## Performance Benefits

### Connection Reduction
- **Without HAProxy**: 100 clients = 100 backend connections
- **With HAProxy + HTTP/2**: 100 clients → ~3-10 backend connections (depending on load balancing)

### Resource Savings
- Fewer TCP handshakes to backends
- Reduced memory footprint on backends
- Better CPU cache utilization
- HTTP/2 header compression (HPACK)

## References

- [RFC 8441 - Bootstrapping WebSockets with HTTP/2](https://datatracker.ietf.org/doc/html/rfc8441)
- [HAProxy WebSocket Documentation](https://www.haproxy.com/blog/websockets-load-balancing-with-haproxy)
- [Go HTTP/2 Extended CONNECT](https://github.com/golang/go/issues/53208)
- [Centrifuge Documentation](https://centrifugal.dev/)
