This example shows how to use custom Broker implementation based on [Nats](https://nats.io/) messaging server. Here Nats handles PUB/SUB stuff while presence information managed by Redis.

Nats Broker does not support any methods related to publication history – it only provides at most once PUB/SUB.

Start Nats server locally:

```
go get github.com/nats-io/nats-server
nats-server
```

Start Redis (for handling presence):

```
redis-server
```

Finally, to start example run the following command from example directory:

```
go run main.go
```

Go to http://localhost:8000. You will see simple chat app, try writing a chat message in one browser tab and you should see it appears in another tab.
