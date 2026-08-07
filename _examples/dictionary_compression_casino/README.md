# Casino / sportsbook traffic reduction

Models a real-time betting workload and measures what connection-level
dictionary compression saves, in bytes and in projected egress cost.

```
go run ./dictionary_compression_casino/
```

## The workload

A representative player's 10 minute session, with four channel types that have
very different fan-out, rates and payload sizes — all mixed on one connection:

| channel | fan-out | rate | payload |
|---|---|---|---|
| `odds:football:major` | large | 4/s | odds board update |
| `odds:tennis:atp` | large | 1.5/s | odds board update |
| `jackpot:global` | largest | 2/s | tiny ticker |
| `table:blackjack:07` | medium | 1/s | full table state |
| `user:#42` | one | 1 per 20s | balance, settlement, bonus |

All five channels are covered by one dictionary, because a dictionary belongs to
a profile — a kind of client — not to a channel. That is what makes the per-user
channel work: on its own it carries too few frames to pay for a dictionary, but
it shares the one the rest of the session already paid for. A returning player
pays nothing at all, having cached it.

## Reading the output

Three sections:

1. **Per-player session, JSON** — bytes on the wire for one player.
2. **Per-player session, Protobuf** — same, binary protocol.
3. **Fan-out** — 120 players on shared markets, showing how often the shared
   frame cache spared the server a compression.

### Caveats

- The fan-out section's *byte* ratio is pessimistic. Running 120 clients plus
  the publisher in one process makes connections fall behind and batch heavily,
  and `permessage-deflate` does comparatively well on large batched frames. A
  real deployment at ~8.5 msg/s per connection writes each message as its own
  frame, which is what the per-player sections measure. Read the fan-out section
  for the **cache hit ratio**, not for the byte ratio.
- Egress pricing is an assumption printed with the output — substitute your own.
- Payload shapes are synthetic. They are modelled on real message types but have
  lower entropy than production traffic, so treat the ratios as an upper bound
  for comparable traffic rather than a promise.
