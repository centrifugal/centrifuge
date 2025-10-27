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

# Generate backend server certificate (localhost + Docker service names)
echo "1. Generating backend server certificate..."
mkcert -cert-file "$CERTS_DIR/localhost.pem" \
       -key-file "$CERTS_DIR/localhost-key.pem" \
       localhost centrifuge1 centrifuge2 centrifuge3 127.0.0.1 ::1

echo "   ✓ Created $CERTS_DIR/localhost.pem"
echo "   ✓ Created $CERTS_DIR/localhost-key.pem"
echo ""

echo ""
echo "✅ Certificate generation complete!"
echo ""
echo "Files created in $CERTS_DIR/:"
ls -lh "$CERTS_DIR/"
echo ""
echo "You can now run: go run main.go"
