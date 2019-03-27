Example demonstrates simple chat with JSON protocol.

Client uses Websocket by default, but you can simply uncomment one line in `index.html` to use SockJS instead. 

To start example locally install dependencies first:

```
dep init
dep ensure
```

Then go to http://localhost:8000 to see Websocket connection in action. You can also connect to this server with GRPC.
