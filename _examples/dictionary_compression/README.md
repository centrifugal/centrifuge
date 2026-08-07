# Connection-level dictionary compression

Measures what the feature actually saves, by counting real bytes delivered from
server to client over a real WebSocket connection.

```
go run ./dictionary_compression/
```

## What it does

For every payload shape it runs three full scenarios end to end — no compression,
`permessage-deflate`, and dictionary compression — and counts bytes at the client
socket, so the numbers include WebSocket framing rather than just protocol
payloads. Every received payload is compared against what was published, and any
mismatch aborts the run.

The payload shapes are unrelated to each other. Each scenario trains its own
dictionary offline from traffic of the same kind, using a separate seed, which is
what a trainer would do from captured traffic. One shape is deliberately
incompressible (random bytes) to show the floor and to exercise the raw-frame
fallback.

## How it works

1. A client advertises support in `ConnectRequest.flag`, and may name a
   `profile` describing what kind of client it is.
2. If `Config.DictionaryCompression` is set, the node asks the engine for a
   dictionary for that profile and protocol. `centrifuge` ships no engine: the
   examples use `_examples/dictionaryengine`, which serves dictionaries handed to
   it at construction.
3. The dictionary travels in `ConnectResult.dict`, and every frame after the
   connect reply is compressed against it. That ordering is the point of
   delivering it there: there is no window in which a compressed frame can reach
   a client that does not hold the dictionary yet.
4. A client caches the dictionary and advertises its id in `ConnectRequest.dict`
   next time. An id is a hash of the content, so when the server recognises it it
   answers with the id alone and sends nothing.

`Push.state` can also carry a dictionary mid-connection. The protocol keeps that
path, but the server does not currently use it.

## Caveats when reading the output

- Steady-state rows exclude the warm-up, so they show the ceiling. The
  whole-session row includes warm-up and the dictionary transfer.
- The shapes are synthetic and have lower entropy than most real traffic; the
  document-state shape in particular keeps 19 of 20 blocks identical between
  revisions, which is favourable. Treat the numbers as an upper bound for
  comparable traffic, not a promise.
- The `deflate` column runs at `CompressionLevel: 6`. That is deliberate:
  `WebsocketConfig.CompressionLevel` defaults to `0`, which is
  `flate.NoCompression` - the connection negotiates permessage-deflate and then
  stores rather than compresses, measuring ~0.99x. Comparing against that default
  overstates dictionary compression by roughly 5x, so do not do it.
- The `built-in` column is the engine enabled with nothing trained for this
  profile, so connections fall back to the protocol structure dictionary, which
  contains no application data. It is the floor a connection gets when no
  dictionary exists for it, and it lands close to a properly configured
  permessage-deflate - the difference between the two is CPU, not bytes (see
  `dictionary_compression_cpu`).
