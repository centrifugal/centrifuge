#!/bin/bash

# Build the server binary for Linux (for Docker container)

set -e

echo "Building server binary for Linux..."

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o server .

echo "✓ Binary built: server"
echo ""
echo "You can now run: docker-compose up --build"
