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

The payload shapes are unrelated to each other and nothing in the server knows
about any of them. The dictionary is built from whatever traffic the node
happens to observe, which is what makes the mechanism general rather than tuned
to one example. One shape is deliberately incompressible (random bytes) to show
the floor and to exercise the raw-frame fallback.

## How it works

1. A client advertises support in `ConnectRequest.flag`. There is no client-side
   option — the server decides.
2. If `Config.DictionaryCompression` is set, the node samples the frames it sends
   and builds a shared dictionary once it has seen enough traffic.
3. The dictionary is **not** sent at connect. It costs its own size in bytes, and
   a connection that only ever receives a handful of frames would spend more on
   the dictionary than it saves. The server waits until the connection has
   carried enough traffic for the dictionary to pay for itself.
4. At that point the server emits a `ConnectionState` push carrying the
   dictionary, then compresses every subsequent frame. The activation frame is
   written immediately before the first compressed frame on the same goroutine,
   so no compressed frame can overtake the dictionary that decodes it.

`ConnectionState` is intentionally generic — every field is independent and
optional, so future connection-level state can be added without a new push type
and without breaking clients that do not understand it.

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
- The `built-in` column is the feature enabled with no channel opted in: only the
  protocol structure dictionary both sides compile in. It is the floor every
  connection gets, and it lands close to a properly configured
  permessage-deflate - the difference between the two is CPU, not bytes (see
  `dictionary_compression_cpu`).
