Example demonstrates a scenario where client calls many subscribe and RPC commands, and we process them concurrently on server side.

Here we emulate potentially slow operation in Subscribe and RPC handlers by sleeping on 500 ms. To avoid blocking client connection read loop we run these slow operations in goroutines. A bounded semaphore inside client connect closure used to limit concurrency of operations running in goroutines for a single client connection.

In real application you may want to use goroutine pools for operations to have a better control on program memory consumption. 

To start example run the following command from example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action.
