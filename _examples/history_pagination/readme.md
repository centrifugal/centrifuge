This example demonstrates a simple chat with manual paginated message recovery from a history stream.

**This example does not cover all edge cases** (error handling, handling disconnects while recovery in process left as an exercise for a user) - it mostly shows the use of API to iterate over history from client side.

Client subscribes on a channel, on first subscribe we save a channel current stream position to a variable, then on reconnect (which is artificially initiated by a server) we try to restore messages from server history – we do this in chunks calling `history` method with `since` and `limit` filter params.

To start example run the following command from example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action.
