Example demonstrates bidirectional WebTransport API interaction using https://github.com/alta/quic-go library.

1) Generate self-signed TLS certificate:
```openssl genrsa -des3 -passout pass:x -out server.pass.key 2048
openssl rsa -passin pass:x -in server.pass.key -out server.key
rm server.pass.key
openssl req -new -key server.key -out server.csr
```

```
openssl x509 -req -sha256 -days 365 -in server.csr -signkey server.key -out server.crt
```

2) To start example run the following command from this directory:
```
go run *.go
```

