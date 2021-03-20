Example demonstrates a simple chat with custom WebSocket transport based on https://github.com/gobwas/ws library. You can expect a much less memory usage when handling lots of connections compared to default Gorilla Websocket based transport (about 3x reduction).

To start example run the following command from example directory:

```
go run *.go
```

Then go to http://localhost:8000 to see it in action.

Note that since this example uses `epoll` on Linux and `kqueue` on BSD-based systems you should be very careful with relying on this code for your production application. **Think wise and carefully test**.
