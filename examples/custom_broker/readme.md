In this example we show how to used custom Broker implementation based on [Nats](https://nats.io/) messaging server. Here Nats handles PUB/SUB stuff while history and presence information managed by Redis.

To start install dependencies first:

```
dep ensure
```

Start Nats server locally:

```
go get github.com/nats-io/gnatsd
gnatsd
```

Start Redis:

```
redis-server
```

Then start example:

```
go run main.go
```

Go to http://localhost:8000. You will see simple chat app, try writing a chat message in one browser tab and you should see it appears in another tab.
