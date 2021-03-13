Example demonstrates a possibility of using Centrifuge with GRPC unidirectional transport. 

The benefit here is that you get the scalable PUB/SUB system for real-time notifications using GRPC ecosystem.

The drawback: you need to use a custom client code to unwrap Centrifuge protocol-encoded push frames (pretty simple though – see example code).

To start server run the following command from example directory:

```
go run main.go
```

Then go to `client` folder and start a client:

```
go run main.go
```

You should see successful connection to a server and different push frames coming from a server.
