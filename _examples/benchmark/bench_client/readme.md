This script intended to be used with [this server](https://github.com/centrifugal/centrifuge/tree/master/_examples/benchmarks/bench_server).

Run it (`n` is total number of messages to publish, `ns` is number of subscribers, `np` is number of publishers):

```
go run main.go -s ws://localhost:8000/connection/websocket -n 1000 -ns 10 -np 1 channel
```

With Protobuf protocol:

```
go run main.go -s ws://localhost:8000/connection/websocket -n 1000 -ns 10 -np 1 -p channel
```

With limited publish rate per second:

```
go run main.go -s ws://localhost:8000/connection/websocket -n 1000 -ns 10 -np 1 -p -pl 30 channel
```

With custom message size:

```
go run main.go -s ws://localhost:8000/connection/websocket -n 1000 -ns 10 -np 1 -p -pl 30 -ms 512 channel
```
