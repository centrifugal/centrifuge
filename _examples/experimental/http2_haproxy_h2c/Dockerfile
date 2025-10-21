FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the pre-built binary and static files
COPY server .
COPY index.html .
COPY certs ./certs

# Install mkcert CA into system trust store
# This ensures the container trusts certificates signed by mkcert
RUN cp /app/certs/rootCA.pem /usr/local/share/ca-certificates/mkcert-rootCA.crt && \
    update-ca-certificates

# Expose the port
EXPOSE 8443

# Run the server
CMD ["./server"]
