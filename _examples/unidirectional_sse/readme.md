Example demonstrates a possibility of using Centrifuge with Eventsource/SSE unidirectional transport. 

The benefit here is that you get the scalable PUB/SUB system for real-time notifications using one of the simplest HTTP-based transports.

The drawback: you need to use a custom client code to unwrap Centrifuge protocol-encoded push frames (pretty simple though).

To start example run the following command from example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action.
