This example shows how to use custom Broker implementation based on [Nats](https://nats.io/) messaging server. Here Nats handles PUB/SUB stuff while history and presence information managed by Redis.

Note that it's also possible to run this example without Redis at all (for example if you only need unreliable but insanely fast and scalable PUB/SUB without message recovery or presence features Redis provides) - just follow comments in `main.go` to disable Redis. 

Start Nats server locally:

```
go get github.com/nats-io/nats-server
nats-server
```

And start Redis:

```
redis-server
```

Finally to start example run the following command from example directory:

```
go run main.go
```

Go to http://localhost:8000. You will see simple chat app, try writing a chat message in one browser tab and you should see it appears in another tab.
