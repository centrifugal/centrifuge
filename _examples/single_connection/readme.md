This example demonstrates how to keep single connection from the same user globally over all Centrifuge nodes.

As soon as user connects we subscribe it to a personal channel with presence enabled. Then inside `OnConnect` handler we check whether user has more than 1 connection inside personal channel at the moment. If yes – we disconnect other user connections (except current one) from a server. 

We also could disconnect all other user connections without using channel presence at all, but this results in more unnecessary disconnect messages travelling around Centrifuge nodes.

To start example run the following command from example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action. Then open another browser tab – as soon as the new connection establishes the previous one will be closed.
