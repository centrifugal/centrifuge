# Measuring the real CPU cost

Answers the only question that a microbenchmark cannot: **how often does the
shared frame cache actually hit under load?**

Frames are shareable between connections only while the writer emits one message
per frame. Once it batches, each connection's frame is a different mix of
messages, the cache misses, and every connection pays for its own compression.

Run the load generator in a **separate process** — thousands of clients in the
server's process steal its CPU and induce exactly the batching being measured.

```
# terminal 1
go run ./dictionary_compression_cpu -mode=server -compress -rate=4 -shared=5 -pool=40
# terminal 2
go run ./dictionary_compression_cpu -mode=client -conns=2000 -shared=5 -pool=40
```

`-pool` larger than `-shared` gives each client a random subset of channels, so
batched frames differ between connections. That is the realistic case and it
matters a lot — see below.

## Measured on an M-series laptop, 1000 connections, 18.6k deliveries/s

Run all three modes to make the comparison mean anything — `-deflate` is the
alternative you would otherwise be using:

```
go run ./dictionary_compression_cpu -mode=server            -rate=4 -shared=5 -pool=40
go run ./dictionary_compression_cpu -mode=server -deflate   -rate=4 -shared=5 -pool=40
go run ./dictionary_compression_cpu -mode=server -compress  -rate=4 -shared=5 -pool=40
```

CPU is core-seconds per million message deliveries, measured with `getrusage` on
the server process only.

| subscriptions | cache hit | none | permessage-deflate | dictionary |
|---|---|---|---|---|
| identical (5 of 5) | 98.8% | 7.15 | 20.78 | **9.63** |
| random (5 of 40) | 37.3% | 7.96 | 20.69 | 24.43 |

The two rows are the whole design, in two numbers:

- **When subscribers share frames**, one compression serves all of them. Deflate
  cannot do that — it holds per-connection state — so it pays per connection and
  costs **2.2x more CPU** than dictionary compression while also sending more
  bytes.
- **When they do not share**, the cache misses, and dictionary compression costs
  about 18% more CPU than deflate for roughly 1.5x fewer bytes.

Heterogeneous subscriptions plus batching are what break sharing. Note the
coupling: batching only happens when the writer falls behind, so compression gets
more expensive exactly when the server is already busy.

> `-deflate` runs at `CompressionLevel: 6`. The `WebsocketConfig` default is `0`
> — `flate.NoCompression` — which negotiates the extension and then stores
> instead of compressing. It looks nearly free in CPU because it is doing
> nothing, and it makes dictionary compression look ~5x better than it is.

The frame cache lives in the engine, not in `centrifuge` — so does the decision
of whether to have one at all, and whether to report its hit rate. The example
engine (`_examples/dictionaryengine`) exposes it through `Stats()`, which is what
makes the numbers above measurable rather than guessed.
