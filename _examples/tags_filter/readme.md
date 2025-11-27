Example demonstrates a simple tags filter usage - filtering messages by tags on the server side according to
filter supplied by client in subscription request.

To start example run the following command from example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action.

## Protobuf mode

The example supports both JSON and Protobuf protocols. By default, it uses JSON protocol with the local centrifuge.js build.

To use Protobuf protocol, add `?protobuf=true` to the URL: http://localhost:8000?protobuf=true

In protobuf mode:
- The example uses `centrifuge.protobuf.js` from unpkg CDN
- Data is encoded/decoded using TextEncoder/TextDecoder
- The server-side code remains the same for both modes
