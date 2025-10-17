# Certificate Verification Checklist

## Certificate Files Generated

```bash
certs/
├── haproxy.pem           # HAProxy frontend cert+key (for client connections)
├── haproxy-cert.pem      # HAProxy certificate only
├── haproxy-key.pem       # HAProxy private key
├── localhost.pem         # Backend server certificate
├── localhost-key.pem     # Backend server private key
└── rootCA.pem           # mkcert root CA (for verification)
```

## Certificate Subject Alternative Names

### Backend Certificate (localhost.pem)
```
DNS:localhost
DNS:centrifuge1        ← Used by HAProxy to connect
DNS:centrifuge2        ← Used by HAProxy to connect
DNS:centrifuge3        ← Used by HAProxy to connect
IP:127.0.0.1
IP:::1
```

### HAProxy Frontend Certificate (haproxy.pem)
```
DNS:localhost          ← Used by browser clients
IP:127.0.0.1
IP:::1
```

## Docker Volume Mounts

### HAProxy Container
```yaml
volumes:
  - ./haproxy/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro
  - ./certs:/certs:ro
```
- Frontend uses: `/certs/haproxy.pem`
- Backend verification uses: `/certs/rootCA.pem`

### Backend Containers (centrifuge1, centrifuge2, centrifuge3)
Certificates are baked into the image during build:
```dockerfile
COPY certs /app/certs
RUN cp /app/certs/rootCA.pem /usr/local/share/ca-certificates/mkcert-rootCA.crt && \
    update-ca-certificates
```
- WORKDIR is `/app`
- Server uses: `certs/localhost.pem` (relative path → `/app/certs/localhost.pem`)
- Server uses: `certs/localhost-key.pem` (relative path → `/app/certs/localhost-key.pem`)
- mkcert CA installed in system trust store: `/etc/ssl/certs/`

## HAProxy Backend Configuration

```
server centrifuge1 centrifuge1:8443 ssl alpn h2 ca-file /certs/rootCA.pem sni str(centrifuge1) verify required check
```

Breakdown:
- `centrifuge1:8443` - Docker service name and port
- `ssl` - Use TLS
- `alpn h2` - Negotiate HTTP/2 via ALPN
- `ca-file /certs/rootCA.pem` - Use this CA to verify backend cert
- `sni str(centrifuge1)` - Send "centrifuge1" as SNI hostname
- `verify required` - Verify hostname matches cert SAN
- `check` - Enable health checks

## Verification Steps

### 1. Check backend certificate includes service names
```bash
openssl x509 -in certs/localhost.pem -text -noout | grep -A 5 "Subject Alternative Name"
```
Should show: `DNS:centrifuge1, DNS:centrifuge2, DNS:centrifuge3`

### 2. Check rootCA.pem exists
```bash
ls -l certs/rootCA.pem
```

### 3. Verify certificate chain
```bash
openssl verify -CAfile certs/rootCA.pem certs/localhost.pem
```
Should output: `certs/localhost.pem: OK`

### 4. Check HAProxy can read certificates
```bash
docker-compose exec haproxy ls -la /certs/
```

### 5. Check backend can read certificates
```bash
docker-compose exec centrifuge1 ls -la /app/certs/
```

### 6. Verify mkcert CA is in system trust store
```bash
docker-compose exec centrifuge1 ls -la /etc/ssl/certs/ | grep mkcert
```
Should show: `mkcert-rootCA.crt` or similar

## Common Issues

### Issue: "connection reset by peer"
- **Cause**: SNI mismatch or missing hostname verification
- **Fix**: Added `sni str(centrifuge1)` and `verify required` to HAProxy config

### Issue: "certificate signed by unknown authority"
- **Cause**: rootCA.pem not accessible to HAProxy
- **Fix**: Ensure `./certs:/certs:ro` is mounted and rootCA.pem exists

### Issue: "bad certificate"
- **Cause**: Backend certificate doesn't include service name in SAN
- **Fix**: Regenerate certs with `centrifuge1, centrifuge2, centrifuge3` in SAN

### Issue: "x509: certificate signed by unknown authority"
- **Cause**: Backend container doesn't trust mkcert CA
- **Fix**: Install rootCA.pem into system trust store during Docker build:
  ```dockerfile
  RUN cp /app/certs/rootCA.pem /usr/local/share/ca-certificates/mkcert-rootCA.crt && \
      update-ca-certificates
  ```
