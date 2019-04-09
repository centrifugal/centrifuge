Example demonstrates oversimplified digest auth workflow over Centrifuge connection.

Server sends random string in Session Push message data and expects it to be transformed to upper case by client and to be sent back to server in Connect command data. If server decides that condition is valid client connection is accepted, otherwise disconnected.

Also note that client library must be configured to wait session message from server which is not by default.

To start example run the following command from example directory:

```
GO111MODULE=on go run main.go
```

Then go to http://localhost:8000 to see it in action.
