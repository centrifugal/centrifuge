Example demonstrates a simple chat manual message recovery from history stream.

**This example does not cover all edge cases** - it just shows the use of API to iterate over history from client side.

Client subscribes on a channel, we save channel current stream position to variable, then on reconnect (which is artificially initiated by a server) we try to restore messages from server history.

To start example run the following command from example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action.
