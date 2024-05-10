This is a sample simulation of football match where the entire state is sent into WebSocket connection upon every
match event.

The goal is to compare different compression strategies for WebSocket data transfer. Please note, that results
depend a lot on the data you send. You may get absolutely different results for your data. Still we hope this
example gives some insights on how to choose the best compression strategy and what to expect from Centrifuge.

Results with different configurations for total data sent over the interface from server to client,
caught with WireShark filter:

```
tcp.srcport == 8000 && websocket
```

| Protocol                     | Compression | Delta     | Bytes sent | Percentage |
|------------------------------|-------------|-----------|------------|------------|
| JSON over JSON               | No          | No        | 40251      | 100.0      |
| JSON over JSON               | Yes         | No        | 15669      | 38.93      |
| JSON over JSON               | No          | Yes       | 6043       | 15.01      |
| JSON over JSON               | Yes         | Yes       | 5360       | 13.32      |
| JSON over Protobuf           | No          | No        | 39180      | 97.34      |
| JSON over Protobuf           | Yes         | No        | 15542      | 38.61      |
| JSON over Protobuf           | No          | Yes       | 4287       | 10.65      |
| JSON over Protobuf           | Yes         | Yes       | 4126       | 10.25      |
| Protobuf over Protobuf       | No          | No        | 16562      | 41.15      |
| Protobuf over Protobuf       | Yes         | No        | 13115      | 32.58      |
| Protobuf over Protobuf       | No          | Yes       | 4382       | 10.89      |
| Protobuf over Protobuf       | Yes         | Yes       | 4473       | 11.11      |
| JSON over JSON 200ms         | Yes         | Yes       | 2060       | 5.12       |
| JSON over Protobuf 200ms     | Yes         | Yes       | 2008       | 4.99       |
| Protobuf over Protobuf 200ms | Yes         | Yes       | 2315       | 5.75       |

Note: since we send JSON over Protobuf, the JSON size is the same as the JSON over JSON case.
In this case Centrifugal protocol gives lower overhead, but the main part comes from the JSON payload size.
Another advantage of JSON over Protobuf is that we are not forced to use base64 encoding for delta case.
