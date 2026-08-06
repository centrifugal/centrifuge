<!--
TODO: this guide predates step 2 and is stale in several places. It still
describes the structure dictionary as compiled into every SDK and free, the
config as MaxDictionaries, and the wire costs from before delivery frames were
compressed. What actually ships now:

  - the structure dictionary is 409 B, SENT by the server and CACHED by the
    client under a content-hash id, not compiled in. It costs 379 B once per
    client and 59 B to re-announce on every later connection.
  - config is MaxChannelDictionaries (0 by default, which is the layer boundary),
    MaxChannelDictionariesPerConn, StructureDictionary, DisableStructureDictionary.
  - channel dictionary delivery is compressed: 4096 B travels in ~460 B on JSON.
  - the break-even margin is 6 and is measured incrementally against the stage a
    connection is already in.
  - MaxChannelDictionariesPerConn is accepted but not yet honoured.

Numbers to re-measure before rewriting: cold 12781 B vs warm 3524 B over 400
messages, casino 6.42x JSON / 5.01x Protobuf, structure dictionary alone 1.42x.
-->

# Dictionary compression — operator guide

Connection-level frame compression against a dictionary derived from your own
traffic. Compared with `permessage-deflate` — configured properly, see the
warning below — it sends **~1.3–1.5x fewer bytes**, and on fan-out workloads it
does so for **~2x less CPU**, because one compression can serve every subscriber.

Every number here was measured, and the measurement is named so you can re-run it.

> **Configure your deflate baseline before comparing.** `WebsocketConfig`
> `CompressionLevel` defaults to `0`, which is `flate.NoCompression`: the
> connection negotiates permessage-deflate and then *stores* instead of
> compressing, costing slightly more than sending nothing. Set it to 6 (or
> whatever you use in production) or every comparison you run will flatter
> dictionary compression by roughly 5x.

---

## Step 0 — it is off, and does nothing

```go
cfg := centrifuge.Config{} // no DictionaryCompression: feature absent
```

Nil engine means no sampling, no dictionaries, no memory, no CPU. Nothing changes
for any client. Enabling it later is safe for a mixed fleet: a client must
advertise support in `ConnectRequest.flag`, and the server confirms in
`ConnectResult.flag`, so older SDKs keep receiving plain frames.

## Step 1 — turn it on, opt nothing in

```go
cfg.DictionaryCompression = centrifuge.NewDictionaryCompressionEngine(
    centrifuge.DictionaryCompressionConfig{})
```

Every supporting connection now uses the **built-in dictionary**: protocol
envelope and generic JSON structure, compiled into the server and every SDK. It
transfers nothing, needs no channel and no entitlement decision, and applies from
the frame *after* connect.

Measured across six unrelated payload shapes (`dictionary_compression`):

| | wire bytes | vs uncompressed |
|---|---|---|
| no compression | 3 173 379 B | 1.00x |
| permessage-deflate (level 6) | 494 577 B | 6.42x |
| built-in dictionary alone | 453 095 B | 7.00x |

So on its own it lands roughly where a correctly configured permessage-deflate
does — while costing far less CPU on fan-out (see below). This is also the floor
every connection gets: the quiet ones that never carry enough traffic to earn a
learned dictionary, and every channel you never opt in.

## Step 2 — opt a channel in

```go
UseChannelDictionary: func(ch string) bool { return ch == "odds:board" },
```

That channel now gets a dictionary built from its own traffic, which is where the
rest of the gain is:

| | wire bytes | vs deflate |
|---|---|---|
| permessage-deflate (level 6) | 494 577 B | 1.00x |
| channel dictionaries | 320 890 B | **1.54x** |

The casino session workload (`dictionary_compression_casino`, one player, 10
minutes, 5130 messages) measures the same shape: deflate 4.92x, dictionary 6.46x
on JSON — **1.31x less traffic**. Protobuf: 3.91x versus 5.05x.

## Step 3 — widen, and set the budget

```go
UseChannelDictionary: func(ch string) bool {
    return strings.HasPrefix(ch, "odds:") || strings.HasPrefix(ch, "market:")
},
MaxDictionaries: 64, // the CPU-for-bandwidth dial
```

Every opted-in channel gets its **own** dictionary — they are never merged — so
`MaxDictionaries` is what bounds the cost. Slots go to the channels responsible
for the most egress (published bytes × subscribers, so a channel with many
subscribers outranks a chatty one nobody listens to), volume decays so slots
follow current traffic rather than historical, and a challenger must beat the
weakest holder by 2x before displacing it. Without that margin, channels of
similar volume trade slots continuously — simulated, 288 evictions became 24.

Raise the number to compress more channels, lower it to spend less CPU.

## Step 4 — watch it

```go
s := engine.Stats()
len(s.Groups)          // dictionaries held — what overhead scales with
s.Candidates           // channels being watched in case they earn a slot
s.FrameCompressions    // frames actually compressed
s.FrameCacheHits       // frames reused across subscribers — the CPU story
```

Client side, `centrifuge-js` and `centrifuge-go` expose `compressionStats()`:
`ratio`, `bytesSaved` (dictionary cost already subtracted), `accepted`, `active`.

---

## The one rule you must get right

**Opting a channel in is a disclosure decision, not a performance one.** It
asserts:

> every message on this channel is safe to show every subscriber, including
> subscribers who join later.

A dictionary is a *verbatim sample of real frames*. It is handed to each
subscriber that earns it, and it is built once and never rebuilt — so someone
joining a year from now receives fragments of messages published today, **even on
a channel that keeps no history**
(`TestDictionaryDisclosesPastTrafficToLateJoiners`).

- ✅ Public feeds anyone may subscribe to — prices, odds, scores, status boards.
- ❌ Private rooms, direct messages, per-user channels, anything whose membership
  changes over time or whose earlier content is not meant for later arrivals.

This is inherent to learning from real traffic, not a gap to be closed: there is
no version of "build a dictionary from your messages" that does not disclose your
messages. Rebuilding periodically would narrow the window, never shut it.

**The built-in dictionary is exempt from all of this.** It contains protocol
structure only — no application data ever entered it — so it is safe on every
channel, which is why it needs no opt-in.

Two things are handled for you:

- **Dictionaries are per channel and never merged**, so opting one channel in can
  never expose it to another channel's subscribers
  (`TestDictionariesNeverMixChannels`).
- **Leaving a channel stops it being a source immediately**, so a connection an
  admin unsubscribed is never handed that channel's dictionary afterwards
  (`TestNoDictionaryFromChannelAfterUnsubscribe`). A dictionary already delivered
  is left alone — those bytes are sent, and its content is frozen at build time.

Channels you leave out are never sampled and never contribute anywhere. They
still benefit: a connection uses its chosen channel's dictionary for *all* its
frames, which is safe because it is entitled to that channel's content.

## What it costs

**CPU** — and this is where the comparison against permessage-deflate actually
turns. Deflate compresses once per connection; dictionary compression compresses
once per *distinct frame* and serves it from a shared cache to every subscriber
that gets the same bytes. So the answer depends entirely on how shareable frames
are. Measured on 1000 connections in two processes, 18.6k deliveries/s
(`dictionary_compression_cpu`), in core-seconds per million deliveries:

| subscriptions | cache hit | none | deflate L6 | dictionary |
|---|---|---|---|---|
| identical (5 of 5) | 98.8% | 7.15 | 20.78 | **9.63** |
| random (5 of 40) | 37.3% | 7.96 | 20.69 | 24.43 |

Read it as: when subscribers share frames, dictionary compression compresses
better than deflate for **less than half the CPU**. When they do not, it costs
about 18% more than deflate for ~1.5x fewer bytes. Heterogeneous subscriptions
plus batching are what break sharing, and note the coupling — batching only
happens when the writer falls behind, so compression gets more expensive exactly
when the server is already busy.

`FrameCacheHits / (FrameCacheHits + FrameCompressions)` tells you which row you
are in, in production.

The cache only helps while frames are shareable. **With `WriteDelay` set, every
connection batches a different mix, nothing matches, and the cache becomes ~3.5%
pure overhead** — set `FrameCacheSize: -1` there.

**Memory** — per dictionary: 4 KB (default), a sample buffer (≤64 KB, freed once
built) and a compressor pool. Per connection: **177 bytes measured**, under 1% of
this repo's 15.7–32.2 KB per-connection baseline. 10k connections on one channel
measured 1.7 MB total and one dictionary; 10k on their own channels stayed at the
`MaxDictionaries` cap, not 10k. The built-in dictionary is one shared instance
per process regardless of connection count.

---

## Behaviour worth knowing

**Compression starts at the second frame.** The connect reply is what tells the
client the feature was accepted, so it goes out unframed; everything after it is
framed with the built-in dictionary. Nothing is transferred to make that happen.

**A channel dictionary is withheld at first.** Unlike the built-in one it costs
its own size in bytes, so a connection must carry enough traffic for the transfer
to pay for itself — judged against the saving that dictionary was *measured* to
achieve on its channel, not a guess. On the wire it is 5537 B on JSON, 4128 B on
Protobuf. Connections that never reach the threshold keep the built-in dictionary,
which is the point: shipping regardless measured **0.52x**, i.e. traffic doubling.

**Frames switch from text to binary.** On a JSON connection the connect reply is
the last text frame; everything after is binary, because compressed payloads are
not valid UTF-8. Proxies and devtools will show the flip.

**Batching still works.** A connection holds one dictionary — from the channel
contributing most of its bytes — and a frame batching several channels is
compressed whole against it. Lossless and safe, with mixed ratio: the dominant
channel matches, the rest get structure-level gain
(`TestBatchedFramesMixChannelsUnderOneDictionary`). Picking a quiet channel
instead measured 19–35% worse, which is why the choice is by egress and not by
subscription order.

**Incompressible payloads back off.** After 32 frames below `BackOffRatio` (0.15)
a connection stops compressing and re-probes every 512 frames. Rarely fires on
JSON, where base64 and the envelope keep the ratio above it.

**Do not lower the compression level.** Below level 2 the DEFLATE encoder ignores
the preset dictionary entirely while still accepting it — silently disabling the
feature. `TestFrameCodecDictionaryActuallyApplies` guards this.

---

## Sizing

`DictionarySize` defaults to 4096. Useful range is ~1.5–6 KB: ratio flattens past
that while CPU keeps climbing, because DEFLATE rehashes the dictionary on every
frame (0.1 KB → 14.5 µs, 5.8 KB → 19.5 µs, 23 KB → 39 µs).

The built-in dictionary is 1186 B and is not configurable — it is a wire format
constant, identical in the server and every SDK, versioned by
`protocol.BuiltinDictionaryID`. Changing its bytes without changing that id would
corrupt frames for older peers rather than fail cleanly
(`TestBuiltinDictionaryIsVersioned`, and `builtin_dictionary.test.ts` decodes
server-produced fixtures in the JS SDK).

## Not yet done

Go and JavaScript SDKs only. The other six need the flag, the built-in dictionary
constant, the `ConnectionState` push and a dictionary-capable inflate (`fflate`
costs 2.1 KB gzipped in the browser) before their users see any of this.
