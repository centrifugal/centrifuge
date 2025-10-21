#!/bin/bash

# Script to generate locally-trusted SSL certificates using mkcert

set -e

CERTS_DIR="./certs"
mkdir -p "$CERTS_DIR"

# Check if mkcert is installed
if ! command -v mkcert &> /dev/null; then
    echo "Error: mkcert is not installed!"
    echo ""
    echo "Please install mkcert first:"
    echo "  macOS:   brew install mkcert"
    echo "  Linux:   See https://github.com/FiloSottile/mkcert#installation"
    echo ""
    exit 1
fi

echo "Generating locally-trusted SSL certificates for HAProxy HTTP/2 example..."
echo ""

# Generate HAProxy certificate
echo "1. Generating HAProxy certificate..."
mkcert -cert-file "$CERTS_DIR/haproxy-cert.pem" \
       -key-file "$CERTS_DIR/haproxy-key.pem" \
       localhost 127.0.0.1 ::1

# HAProxy requires cert and key in single file
cat "$CERTS_DIR/haproxy-cert.pem" "$CERTS_DIR/haproxy-key.pem" > "$CERTS_DIR/haproxy.pem"

echo "   ✓ Created $CERTS_DIR/haproxy.pem (combined cert + key)"
echo ""

# Copy mkcert root CA for Docker containers
echo "2. Copying mkcert root CA..."
MKCERT_CAROOT=$(mkcert -CAROOT)
if [ -f "$MKCERT_CAROOT/rootCA.pem" ]; then
    cp "$MKCERT_CAROOT/rootCA.pem" "$CERTS_DIR/rootCA.pem"
    echo "   ✓ Created $CERTS_DIR/rootCA.pem"
else
    echo "   ⚠️  Warning: Could not find mkcert root CA at $MKCERT_CAROOT/rootCA.pem"
    echo "   You may need to run 'mkcert -install' first"
fi
echo ""

echo ""
echo "✅ Certificate generation complete!"
echo ""
echo "Files created in $CERTS_DIR/:"
ls -lh "$CERTS_DIR/"
echo ""
echo "You can now run: ./build.sh && docker-compose up --build"
