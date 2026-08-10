# Dictionary compression

A minimal `centrifuge.DictionaryCompression` implementation: one dictionary,
built at startup, served to every client that can decode one.

```bash
go run main.go
```

Then connect a client that supports dictionary compression (`centrifuge-go` or
`centrifuge-js`) to `ws://localhost:8000/connection/websocket` and subscribe to
`market`.

## What Centrifuge gives you, and what it doesn't

Centrifuge handles the wire: it advertises support, carries a dictionary in the
connect reply, installs the codec on the connection, and marks every frame as
compressed or not so a client always knows what it received.

It ships no implementation, and that is deliberate. A dictionary is sent to
clients, so whatever goes into one is disclosed to them — deciding its contents,
which connection receives which dictionary, and when one is withdrawn are
product decisions with real consequences. They belong to whoever supplies the
engine.

This example takes the simplest possible position on all three, and the
dictionary is hardcoded from data that belongs to the application rather than to
any user. [Centrifugo PRO](https://centrifugal.dev/docs/pro/bandwidth_optimizations#dictionary-compression)
takes the other end: it trains dictionaries from live traffic, has a human
approve every value that goes in, serves them per audience, and can stage or
withdraw a version without a restart.

## If you build on this

Two things decide what the feature costs, and only the first is in this file:

- **Compress a fan-out once.** One publication reaching a thousand subscribers
  should be one compression. The cache here does that for frames it has already
  seen.
- **Collapse concurrent duplicates.** Those subscribers are written by that many
  goroutines at nearly the same instant, so they all miss the cache — the first
  has not stored its result yet — and all compress the same bytes. Measured on a
  four-subscriber channel, every frame was compressed three or four times over
  while the cache reported a hit rate that looked fine. Have the first arrival
  compress and the rest wait for it. On that workload it removed half the
  compressions and a third of the server's CPU.

The second is left out here to keep the example about the interface.
