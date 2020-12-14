In this example we show how to use `Node.Survey` to collect information from all running Centrifuge nodes.

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

Go to http://localhost:8000 and to http://localhost:8001 in another browser tab. Clients will subscribe to channels (each to its `window.location.host`), each second survey will ask for all active channels from all running nodes.
