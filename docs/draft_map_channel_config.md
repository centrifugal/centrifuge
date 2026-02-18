# Map Channel Configuration

Map channels require explicit configuration via `GetMapChannelOptions`. Channels without configuration return an error — there are no implicit defaults.

## Modes

Every map channel is defined by two orthogonal modes:

### SyncMode (required)

Controls how client state stays in sync with server state after disconnections.

| Value | Behavior |
|---|---|
| `MapSyncEphemeral` | No stream. Live updates via PUB/SUB only. Missed updates during disconnect are lost. Reconnect = full state resync. |
| `MapSyncConverging` | Stream-backed. Missed updates are recovered from stream on reconnect. Client state converges to server state via delta catch-up. |

### RetentionMode (required)

Controls the data lifecycle of entries in the channel.

| Value | Behavior |
|---|---|
| `MapRetentionExpiring` | Entries expire after `KeyTTL`. KeyTTL must be > 0. Cleanup worker removes expired entries and publishes removal events. |
| `MapRetentionPermanent` | Entries live forever (until explicitly removed). KeyTTL must be 0. |

## Combinations

All four combinations are valid:

| | Expiring | Permanent |
|---|---|---|
| **Ephemeral** | Presence, cursors, typing indicators | Game settings, room config |
| **Converging** | Lobbies, rooms with inactivity timeout | Task boards, polls, leaderboards |

### Ephemeral + Expiring

Entries appear and disappear based on TTL. Clients get live updates but no recovery on reconnect. Typical for presence — clients publish heartbeats with short TTL, other clients see who's online.

```go
centrifuge.MapChannelOptions{
    SyncMode:      centrifuge.MapSyncEphemeral,
    RetentionMode: centrifuge.MapRetentionExpiring,
    KeyTTL:        60 * time.Second,
}
```

No stream. No offsets. No MetaTTL.

### Ephemeral + Permanent

Permanent state with best-effort live delivery. Clients get full state on subscribe and live updates, but no recovery of missed updates. Suitable for rarely-changing data where full resync on reconnect is acceptable.

```go
centrifuge.MapChannelOptions{
    SyncMode:      centrifuge.MapSyncEphemeral,
    RetentionMode: centrifuge.MapRetentionPermanent,
}
```

No stream. No offsets. No MetaTTL.

### Converging + Expiring

Entries expire via TTL, but missed expirations and updates are recovered from the stream on reconnect. Client state converges to server state. Useful for temporary resources that clients must track reliably.

```go
centrifuge.MapChannelOptions{
    SyncMode:      centrifuge.MapSyncConverging,
    RetentionMode: centrifuge.MapRetentionExpiring,
    KeyTTL:        5 * time.Minute,
}
```

Stream defaults (when zero): StreamSize=100, StreamTTL=1min, MetaTTL=StreamTTL*10.

### Converging + Permanent

Permanent state with reliable delivery. Missed updates during disconnect are recovered from the stream. The primary mode for collaborative/shared state where data matters and clients must stay in sync.

```go
centrifuge.MapChannelOptions{
    SyncMode:      centrifuge.MapSyncConverging,
    RetentionMode: centrifuge.MapRetentionPermanent,
    Ordered:       true,
}
```

Stream defaults (when zero): StreamSize=100, StreamTTL=1min, MetaTTL=permanent (no expiry).

## Options Reference

```go
type MapSyncMode int

const (
    MapSyncEphemeral  MapSyncMode = iota + 1
    MapSyncConverging
)

type MapRetentionMode int

const (
    MapRetentionExpiring  MapRetentionMode = iota + 1
    MapRetentionPermanent
)

type MapChannelOptions struct {
    // SyncMode controls client-server synchronization after disconnections.
    // Required. Zero value = not configured = error.
    SyncMode MapSyncMode

    // RetentionMode controls the data lifecycle of entries.
    // Required. Zero value = not configured = error.
    RetentionMode MapRetentionMode

    // KeyTTL sets automatic expiration for entries in this channel.
    // Required when RetentionMode is Expiring (must be > 0).
    // Must be 0 when RetentionMode is Permanent.
    KeyTTL time.Duration

    // Ordered enables score-based ordering. When true, entries are
    // returned sorted by Score (descending). Orthogonal to both modes.
    Ordered bool

    // --- Advanced (Converging mode only, zero = auto-derived) ---

    // StreamSize sets the maximum number of entries in the recovery stream.
    // Zero = auto-derived (100). Must be 0 when SyncMode is Ephemeral.
    StreamSize int

    // StreamTTL sets how long stream entries are retained.
    // Zero = auto-derived (1 minute). Must be 0 when SyncMode is Ephemeral.
    StreamTTL time.Duration

    // MetaTTL sets how long stream metadata (epoch, offset) is retained.
    // Zero = auto-derived:
    //   Converging + Expiring:  StreamTTL * 10
    //   Converging + Permanent: permanent (no expiry)
    // Must be 0 when SyncMode is Ephemeral.
    MetaTTL time.Duration
}
```

## Auto-derivation (Converging mode, zero values)

| Field | Expiring | Permanent |
|---|---|---|
| StreamSize | 100 | 100 |
| StreamTTL | 1 minute | 1 minute |
| MetaTTL | StreamTTL * 10 | permanent |

These defaults provide reasonable recovery out of the box. StreamSize=100 handles typical reconnect scenarios without full state resync. Recovery failure gracefully falls back to full state resync — not broken, just less efficient. Tune StreamSize and StreamTTL for your traffic patterns.

## Validation

The resolver is called on every operation (subscribe, publish, read, remove). The result is validated, auto-derived values are filled in, and used for that operation. No caching — each call is independent.

Validation is just a few comparisons — negligible cost next to a Redis/PG roundtrip. Invalid configuration returns an error to the caller immediately.

### Mode validation

| Condition | Error |
|---|---|
| `GetMapChannelOptions` is nil | "map channel options resolver not configured" |
| `SyncMode == 0` | "map channel not configured: set SyncMode" |
| `RetentionMode == 0` | "map channel not configured: set RetentionMode" |
| Unknown `SyncMode` value | "invalid SyncMode value" |
| Unknown `RetentionMode` value | "invalid RetentionMode value" |

### Retention validation

| Condition | Error |
|---|---|
| `Expiring` + `KeyTTL == 0` | "KeyTTL required for RetentionMode Expiring" |
| `Expiring` + `KeyTTL < 0` | "KeyTTL must be positive" |
| `Permanent` + `KeyTTL > 0` | "KeyTTL must be 0 for RetentionMode Permanent (entries don't expire)" |

### Sync validation

| Condition | Error |
|---|---|
| `Ephemeral` + `StreamSize > 0` | "StreamSize requires SyncMode Converging" |
| `Ephemeral` + `StreamTTL > 0` | "StreamTTL requires SyncMode Converging" |
| `Ephemeral` + `MetaTTL > 0` | "MetaTTL requires SyncMode Converging" |
| `Converging` + `StreamSize < 0` | "StreamSize must be non-negative" |
| `Converging` + `StreamTTL < 0` | "StreamTTL must be non-negative" |
| `Converging` + `MetaTTL < 0` | "MetaTTL must be non-negative" |
| `Converging` + explicit `MetaTTL` < explicit `StreamTTL` | "MetaTTL must be >= StreamTTL (metadata must outlive stream)" |

### Ordered

`Ordered` is valid with all four mode combinations. No additional validation — it only affects read-side sort order.

### Subscribe-side behavior

SyncMode drives subscribe behavior automatically:

- `Converging` → `EnableRecovery` and `EnablePositioning` are set automatically. No user flags needed.
- `Ephemeral` → neither is set. Reconnect always delivers full state.

`EnableRecovery`/`EnablePositioning` in `SubscribeOptions` are ignored for map subscriptions. The channel config is the single source of truth.

## Resolver

```go
GetMapChannelOptions func(channel string) MapChannelOptions
```

If `GetMapChannelOptions` is nil on the node config, any map operation returns an error.

If the resolver returns `MapChannelOptions{}` (zero value), both modes are 0, which triggers the "not configured" validation error. This acts as a natural allowlist — only explicitly configured channels can be used as map channels.

```go
GetMapChannelOptions: func(channel string) centrifuge.MapChannelOptions {
    switch channel {
    case "board":
        return centrifuge.MapChannelOptions{
            SyncMode:      centrifuge.MapSyncConverging,
            RetentionMode: centrifuge.MapRetentionPermanent,
            Ordered:       true,
        }
    case "cursors":
        return centrifuge.MapChannelOptions{
            SyncMode:      centrifuge.MapSyncEphemeral,
            RetentionMode: centrifuge.MapRetentionExpiring,
            KeyTTL:        5 * time.Second,
        }
    default:
        if strings.HasPrefix(channel, "poll:") {
            return centrifuge.MapChannelOptions{
                SyncMode:      centrifuge.MapSyncConverging,
                RetentionMode: centrifuge.MapRetentionPermanent,
            }
        }
        return centrifuge.MapChannelOptions{} // → error: not configured
    }
}
```

## Per-operation overrides

Stream configuration (`StreamSize`, `StreamTTL`, `MetaTTL`) is channel-level only. No per-operation overrides — stream topology should not vary per write.

`KeyTTL` is channel-level only. Different key lifetimes should use different channels. Internal subsystems that currently set `KeyTTL` per-operation (e.g., map-based presence via `MapPublishOptions.KeyTTL`) already use separate channels per concern, so the TTL naturally moves to the channel config.

Per-operation options (`MapPublishOptions`, `MapRemoveOptions`) retain only non-channel concerns: idempotency, CAS, score, data, tags, etc.

## Migration from current API

| Before | After |
|---|---|
| `StreamSize > 0` + `EnableRecovery` in subscribe opts | `SyncMode: MapSyncConverging` |
| `StreamSize == 0` (streamless) | `SyncMode: MapSyncEphemeral` |
| `KeyTTL > 0` | `RetentionMode: MapRetentionExpiring` + `KeyTTL: ...` |
| `KeyTTL <= 0` or omitted | `RetentionMode: MapRetentionPermanent` |
| `DefaultMapChannelOptions()` | Remove — configure each channel explicitly |
| `StreamSize`/`StreamTTL`/`MetaTTL` on publish/remove opts | Remove — channel-level only |
