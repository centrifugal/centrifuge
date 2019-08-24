Example demonstrates simple chat with custom WebSocket transport based on https://github.com/gobwas/ws library. You can expect a much less memory usage when handling lots of connections compared to default Gorilla Websocket based transport (about 3x reduction).

To start example run the following command from example directory:

```
GO111MODULE=on go run main.go
```

Then go to http://localhost:8000 to see it in action.
