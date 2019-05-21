Example demonstrates how to build simple event bus with custom Eventsource transport.

There is no additional client required for client side as no complex bidirectional logic involved – client subscribed on channel server-side and just receives everything that was published into channel.

```
GO111MODULE=on go run main.go
```

Then go to http://localhost:8000 to see it in action.
