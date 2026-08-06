# Dictionary compression — browser demo

Shows what connection-level dictionary compression does to real traffic, live in
a browser, with a toggle between server modes and between JSON and Protobuf.

## Running

Two servers. First the SDK bundles, from a `centrifuge-js` checkout on the
`dictionary-compression` branch:

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

The server runs **two Centrifuge nodes side by side** — one with compression,
one without — publishing the identical feed into both. The page picks an
endpoint, so switching mode compares like with like:

| mode | endpoint |
|---|---|
| no compression | `/connection/websocket` |
| permessage-deflate | `/connection/websocket/deflate` |
| dictionary compression | `/connection/websocket/compressed` |

The feed is a live odds board: ~20 small JSON messages a second, which is the
shape where this helps most.

The **side by side** table keeps the best sample per mode/protocol pair, so you
can run each in turn and compare bytes per message directly.

The **server side** panel polls `DictionaryCompressionEngine.Stats()` and shows
the frame cache hit rate — frames the server compressed once and reused for
other subscribers. Do not expect much from a few browser tabs: the rate scales
with fan-out, measured 0% at 3 subscribers, 68% at 30 and 99% at 2000. The cache
buffers its writes, so at tiny fan-out every subscriber looks up the frame before
the first store lands. That is fine — with three subscribers there is almost
nothing to save.

## The connection switches from text to binary

On a JSON connection frames start as WebSocket **text** messages. The
`ConnectionState` frame carrying the dictionary is the last text frame; every
frame after it is **binary**, because a compressed payload is arbitrary bytes and
a text frame must be valid UTF-8. Observed on the wire:

```
msg #1    text
msg #61   text     <- ConnectionState carrying the dictionary
msg #62   BINARY   marker=0x01
```

Protobuf connections were binary already, so nothing changes there. Anything
inspecting the stream — a proxy, browser devtools — will show the flip mid
connection.

## Reading the numbers

- Give dictionary mode ~10 seconds. The dictionary is only sent once a
  connection has carried enough traffic to earn it back, so the first seconds
  are deliberately uncompressed and drag the whole-session average down.
- **Compression ratio** is steady state — compressed frames only. **Net saved**
  is the whole connection, with the dictionary transfer subtracted, so it is the
  honest figure and can start negative.
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
