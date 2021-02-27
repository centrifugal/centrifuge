In this example we show how `Node.Subscribe` and `Node.Unsubscribe` can be used to control server-side subscriptions of a user.

Start Redis server locally:

```
redis-server
```

Start first example instance on port 8000:

```
go run main.go -port 8000
```

Then start another example instance on port 8001 (in another terminal):

```
go run main.go -port 8001
```

Go to http://localhost:8000 and to http://localhost:8001 in another browser tab.

Server will periodically subscribe and unsubscribe user to/from a channel `blink182`.
