This is a sample simulation of football match where the entire state is sent into WebSocket connection upon every
match event. The example is not very idiomatic because we try to simulate various modes thus several different
files were required. In practice, you will have JSON or Protobuf case only, and there is no need to tweak behaviour
over URL params like we do here.

The goal was to compare different compression strategies for WebSocket data transfer. Please note, that results
depend a lot on the data you send. You may get absolutely different results for your data. Still we hope this
example gives some insights on how to choose the best compression strategy and what to expect from Centrifuge.

Results with different configurations for total data sent over the interface from server to client,
caught with WireShark filter:

```
tcp.srcport == 8000 && websocket
```

| Protocol                   | Compression | Delta      | Delay | Bytes sent | Percentage |
|----------------------------|-------------|------------|-------|------------|-----------|
| JSON over JSON             | No          | No         | 0     | 40251      | 100.0     |
| JSON over JSON             | Yes         | No         | 0     | 15669      | 38.93     |
| JSON over JSON             | No          | Yes        | 0     | 6043       | 15.01     |
| JSON over JSON             | Yes         | Yes        | 0     | 5360       | 13.32     |
| JSON over Protobuf         | No          | No         | 0     | 39180      | 97.34     |
| JSON over Protobuf         | Yes         | No         | 0     | 15542      | 38.61     |
| JSON over Protobuf         | No          | Yes        | 0     | 4287       | 10.65     |
| JSON over Protobuf         | Yes         | Yes        | 0     | 4126       | 10.25     |
| Protobuf over Protobuf     | No          | No         | 0     | 16562      | 41.15     |
| Protobuf over Protobuf     | Yes         | No         | 0     | 13115      | 32.58     |
| Protobuf over Protobuf     | No          | Yes        | 0     | 4382       | 10.89     |
| Protobuf over Protobuf     | Yes         | Yes        | 0     | 4473       | 11.11     |
| JSON over JSON             | Yes         | Yes        | 200ms | 2060       | 5.12      |
| JSON over Protobuf         | Yes         | Yes        | 200ms | 2008       | 4.99      |
| Protobuf over Protobuf     | Yes         | Yes        | 200ms | 2315       | 5.75      |

Note: since we send JSON over Protobuf, the JSON size is the same as the JSON over JSON case.
In this case Centrifugal protocol gives lower overhead, but the main part comes from the JSON payload size.
Another advantage of JSON over Protobuf is that we are not forced to use base64 encoding for delta case.
