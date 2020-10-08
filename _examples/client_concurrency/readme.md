Example demonstrates a scenario where client calls many subscribe and RPC commands.

In this example we artificially sleep in Subscribe and RPC handlers to demonstrate that response times on frontend side are reasonable. This is achieved since Centrifuge processes client commands in parallel (with concurrency limit for a single connection).

To start example run the following command from example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action.
