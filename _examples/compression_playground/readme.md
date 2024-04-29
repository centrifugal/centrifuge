This is a sample simulation of football match where the entire state is sent into WebSocket connection upon every
match event.

Results with different configurations for total data sent over the interface from server to client,
caught with WireShark filter:

```
tcp.srcport == 8000 && websocket
```

| Protocol                    | Compression | Delta     | Bytes sent | Percentage |
|-----------------------------|-------------|-----------|------------|------------|
| JSON over JSON              | No          | No        | 29510      | 100.0      | 
| JSON over JSON              | Yes         | No        | 11135      | 37.73      | 
| JSON over JSON              | No          | Yes       | 6435       | 21.81      |
| JSON over JSON              | Yes         | Yes       | 4963       | 16.82      |
| JSON over Protobuf          | No          | No        | 28589      | 96.88      |
| JSON over Protobuf          | Yes         | No        | 11133      | 37.73      |
| JSON over Protobuf          | No          | Yes       | 4276       | 14.49      |
| JSON over Protobuf          | Yes         | Yes       | 3454       | 11.70      |
| Protobuf over Protobuf      | No          | No        | ?          | ?          |
| Protobuf over Protobuf      | Yes         | No        | ?          | ?          |
| Protobuf over Protobuf      | No          | Yes       | ?          | ?          |
| Protobuf over Protobuf      | Yes         | Yes       | ?          | ?          |
| Protobuf over Protobuf      | Yes         | Yes       | ?          | ?          |

Note: since we send JSON over Protobuf, the JSON size is the same as the JSON over JSON case.
In this case Centrifugal protocol gives lower overhead, but the main part comes from the JSON payload size.
Another advantage of JSON over Protobuf is that we are not forced to use base64 encoding for delta case.
