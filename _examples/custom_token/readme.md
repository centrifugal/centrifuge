Example demonstrates a possibility to use your own custom token parsing logic for client-side refresh workflow.

In this example we expect a string with user ID `I am <ID>` from client as token to continue working with it. **This is just an example, it's an absolutely insecure approach, never do this in production!**

To start an example run the following command from example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action.
