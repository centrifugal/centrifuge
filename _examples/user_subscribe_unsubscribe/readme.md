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

On start user will be subscribed on server-side to a channel `blink182`. After you click on a screen Centrifuge client will issue an RPC call to a server, a server will then unsubscribe user, then subscribe it back (after 1-second delay). Both browser tabs should receive subscribe/unsubscribe events.
