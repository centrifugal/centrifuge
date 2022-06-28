Example demonstrates an experimental bidirectional emulation feature.

Server has several bidirectional endpoints:

1. WebSocket
2. Http stream endpoint (which works over emulation layer)
3. SSE endpoint (which works over emulation layer)

To start example run the following command from example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action.
