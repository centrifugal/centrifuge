Connect to Centrifuge GRPC client server over GRPC.

Without TLS:

```
go run main.go -addr localhost:8001 -channel chat:index
```

With TLS:

```
go run main.go -addr localhost:443 -channel chat:index -tls
```
