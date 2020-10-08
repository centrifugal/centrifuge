Example demonstrates a scenario where client calls many commands over protocol.

Also in this example we artificially sleep in Subscribe and RPC handlers to see reasonable response times on frontend side. This is achieved since Centrifuge processes client commands in parallel (with default concurrency limit for a single connection).

To start example run the following command from example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action.
