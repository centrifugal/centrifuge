Example demonstrates oversimplified digest auth workflow. Server sends random string to client and expects it to be transformed to upper case by client.

Client uses Websocket by default, but you can simply uncomment one line in `index.html` to use SockJS instead. 

To start example locally install dependencies first:

```
dep init
dep ensure
```

Then go to http://localhost:8000 to see it in action.
