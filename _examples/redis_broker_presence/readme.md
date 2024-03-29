In this example we show how to connect several Centrifuge server nodes with Redis – thus making it possible to scale nodes.

Start 2 Redis servers locally (we could use just one actually, but here we show builtin Redis sharding feature):

```
redis-server --port 6379
```

And another one on port `6380`:

```
redis-server --port 6380
```

Then start 2 instances of this app on different ports:

```
go run main.go -port 8000
```

And:

```
go run main.go -port 8001
```

Then go to http://localhost:8000 and to http://localhost:8001 in another browser tab. You will see simple chat app in both browser tabs, try writing a chat message in one browser tab – you should see it appears in another tab.

This example uses two Redis instances so published messages will be automatically sharded among them (consistently sharded by a channel).
