# Dictionary compression — browser demo

Shows what connection-level dictionary compression does to real traffic, live in
a browser, with a toggle between server modes and between JSON and Protobuf.

## Running

Two servers. First the SDK bundles, from a `centrifuge-js` checkout on the
`dictionary_compression` branch:

```
yarn dev            # serves both bundles on http://localhost:2000, both watched
```

That builds `centrifuge.js` and `centrifuge.protobuf.js`, watches both, and
serves them from one port — which is what the protocol toggle on the page needs.
Any static server over `dist/` works too, if both bundles were built already.

Then the demo:

```
go run ./dictionary_compression_browser
```

Open <http://localhost:8400>.

## What it shows

The server runs **four Centrifuge nodes side by side** — plain,
permessage-deflate, structure dictionary and profile dictionary — publishing the
identical feed into all of them. The page picks an
endpoint, so switching mode compares like with like:

| mode | endpoint |
|---|---|
| no compression | `/connection/websocket` |
| permessage-deflate | `/connection/websocket/deflate` |
| structure dictionary | `/connection/websocket/structure` |
| profile dictionary | `/connection/websocket/compressed` |

The last two are the same feature at its two tiers. `structure` runs an engine
with nothing trained for any profile, so connections fall back to the protocol
structure dictionary — no application data in it, no review, nothing for an
operator to decide. `compressed` serves a dictionary trained from this feed. The
gap between those two rows is what training is worth.

The feed is a live odds board: ~20 small JSON messages a second, which is the
shape where this helps most.

The **side by side** table keeps the best sample per mode/protocol pair, so you
can run each in turn and compare bytes per message directly.

The page declares `profile: 'odds-board'` at connect, and the server answers with
the dictionary trained for that profile. `centrifuge` ships no trainer and no
dictionaries: the demo uses `_examples/dictionaryengine`, which is handed a
dictionary built from the same feed at startup.

The **server side** panel polls the example engine's `Stats()` and shows
the frame cache hit rate — frames the server compressed once and reused for
other subscribers. Do not expect much from a few browser tabs: the rate scales
with fan-out, measured 0% at 3 subscribers, 68% at 30 and 99% at 2000. The cache
buffers its writes, so at tiny fan-out every subscriber looks up the frame before
the first store lands. That is fine — with three subscribers there is almost
nothing to save.

## The connection switches from text to binary

On a JSON connection the connect reply is a WebSocket **text** message, and it is
the last one: it carries the dictionary, so every frame after it is **binary**,
because a compressed payload is arbitrary bytes and a text frame must be valid
UTF-8. Observed on the wire:

```
msg #1    text     <- connect reply, carrying the dictionary
msg #2    BINARY   marker=0x01
```

Protobuf connections were binary already, so nothing changes there. Anything
inspecting the stream — a proxy, browser devtools — will show the flip mid
connection.

## Reading the numbers

- Compression starts immediately: the dictionary rides in the connect reply, so
  there is nothing to wait for.
- **Compression ratio** is steady state — compressed frames only. **Net saved**
  is the whole connection, with the dictionary transfer subtracted, so it is the
  honest figure and starts negative until the transfer is earned back. Reload the
  page and it starts positive: the client cached the dictionary and the server
  sends only its id.
- Byte counts come from the **server**, measured at the socket after
  compression. They have to: a browser cannot see permessage-deflate at all,
  because the WebSocket API inflates those frames before JavaScript gets them, so
  an in-page count would report deflate as saving exactly nothing — an artefact
  of the measurement, not a result. Counts are per mode across all tabs, so
  bytes ÷ messages stays correct however many you open.
- permessage-deflate runs at `CompressionLevel: 6` here. The default is `0`,
  which is `flate.NoCompression`: the connection negotiates the extension and
  then stores rather than compresses, measuring slightly *worse* than no
  compression at all. It is an easy default to leave in place and it makes any
  comparison meaningless, so the demo sets it explicitly.
