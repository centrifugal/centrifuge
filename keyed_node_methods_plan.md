# Plan: Node-Level Keyed Engine Methods with Singleflight

## Overview

Add Node-level methods for KeyedEngine operations that use singleflight when `UseSingleFlight` config is enabled, following the existing pattern used for History and Presence.

---

## Current Pattern (Reference)

### History Example (node.go)

```go
// Singleflight groups at package level
var (
    presenceGroup      singleflight.Group
    presenceStatsGroup singleflight.Group
    historyGroup       singleflight.Group
)

// Node.History method
func (n *Node) History(ch string, opts ...HistoryOption) (HistoryResult, error) {
    // ... setup ...

    if n.config.UseSingleFlight {
        var builder strings.Builder
        builder.WriteString(ch)
        if historyOpts.Filter.Since != nil {
            builder.WriteString(",offset:")
            builder.WriteString(strconv.FormatUint(historyOpts.Filter.Since.Offset, 10))
            builder.WriteString(",epoch:")
            builder.WriteString(historyOpts.Filter.Since.Epoch)
        }
        builder.WriteString(",limit:")
        builder.WriteString(strconv.Itoa(historyOpts.Filter.Limit))
        builder.WriteString(",reverse:")
        builder.WriteString(strconv.FormatBool(historyOpts.Filter.Reverse))
        key := builder.String()

        result, err, _ := historyGroup.Do(key, func() (any, error) {
            return n.history(ch, historyOpts)
        })
        return result.(HistoryResult), err
    }
    return n.history(ch, historyOpts)
}
```

---

## Implementation Plan

### 1. Add Singleflight Groups (node.go)

```go
var (
    presenceGroup       singleflight.Group
    presenceStatsGroup  singleflight.Group
    historyGroup        singleflight.Group
    // NEW:
    keyedSnapshotGroup  singleflight.Group
    keyedStreamGroup    singleflight.Group
    keyedStatsGroup     singleflight.Group
)
```

### 2. Add Node Methods

#### 2.1 KeyedSnapshot

```go
// KeyedSnapshotResult wraps keyed snapshot result.
type KeyedSnapshotResult struct {
    Publications []*Publication
    Position     StreamPosition
    Cursor       string
}

// KeyedSnapshot retrieves keyed snapshot for a channel.
func (n *Node) KeyedSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) (KeyedSnapshotResult, error) {
    if n.keyedEngine == nil {
        return KeyedSnapshotResult{}, ErrorNotAvailable
    }

    if n.config.UseSingleFlight {
        key := n.keyedSnapshotKey(ch, opts)
        result, err, _ := keyedSnapshotGroup.Do(key, func() (any, error) {
            pubs, pos, cursor, err := n.keyedEngine.ReadSnapshot(ctx, ch, opts)
            if err != nil {
                return KeyedSnapshotResult{}, err
            }
            return KeyedSnapshotResult{
                Publications: pubs,
                Position:     pos,
                Cursor:       cursor,
            }, nil
        })
        if err != nil {
            return KeyedSnapshotResult{}, err
        }
        return result.(KeyedSnapshotResult), nil
    }

    pubs, pos, cursor, err := n.keyedEngine.ReadSnapshot(ctx, ch, opts)
    if err != nil {
        return KeyedSnapshotResult{}, err
    }
    return KeyedSnapshotResult{
        Publications: pubs,
        Position:     pos,
        Cursor:       cursor,
    }, nil
}

func (n *Node) keyedSnapshotKey(ch string, opts KeyedReadSnapshotOptions) string {
    var builder strings.Builder
    builder.WriteString(ch)
    builder.WriteString(",cursor:")
    builder.WriteString(opts.Cursor)
    builder.WriteString(",limit:")
    builder.WriteString(strconv.Itoa(opts.Limit))
    builder.WriteString(",ordered:")
    builder.WriteString(strconv.FormatBool(opts.Ordered))
    builder.WriteString(",key:")
    builder.WriteString(opts.Key)
    if opts.Revision != nil {
        builder.WriteString(",rev_offset:")
        builder.WriteString(strconv.FormatUint(opts.Revision.Offset, 10))
        builder.WriteString(",rev_epoch:")
        builder.WriteString(opts.Revision.Epoch)
    }
    return builder.String()
}
```

#### 2.2 KeyedStream

```go
// KeyedStreamResult wraps keyed stream result.
type KeyedStreamResult struct {
    Publications []*Publication
    Position     StreamPosition
}

// KeyedStream retrieves keyed stream for a channel.
func (n *Node) KeyedStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) (KeyedStreamResult, error) {
    if n.keyedEngine == nil {
        return KeyedStreamResult{}, ErrorNotAvailable
    }

    if n.config.UseSingleFlight {
        key := n.keyedStreamKey(ch, opts)
        result, err, _ := keyedStreamGroup.Do(key, func() (any, error) {
            pubs, pos, err := n.keyedEngine.ReadStream(ctx, ch, opts)
            if err != nil {
                return KeyedStreamResult{}, err
            }
            return KeyedStreamResult{
                Publications: pubs,
                Position:     pos,
            }, nil
        })
        if err != nil {
            return KeyedStreamResult{}, err
        }
        return result.(KeyedStreamResult), nil
    }

    pubs, pos, err := n.keyedEngine.ReadStream(ctx, ch, opts)
    if err != nil {
        return KeyedStreamResult{}, err
    }
    return KeyedStreamResult{
        Publications: pubs,
        Position:     pos,
    }, nil
}

func (n *Node) keyedStreamKey(ch string, opts KeyedReadStreamOptions) string {
    var builder strings.Builder
    builder.WriteString(ch)
    if opts.Filter.Since != nil {
        builder.WriteString(",since_offset:")
        builder.WriteString(strconv.FormatUint(opts.Filter.Since.Offset, 10))
        builder.WriteString(",since_epoch:")
        builder.WriteString(opts.Filter.Since.Epoch)
    }
    builder.WriteString(",limit:")
    builder.WriteString(strconv.Itoa(opts.Filter.Limit))
    builder.WriteString(",reverse:")
    builder.WriteString(strconv.FormatBool(opts.Filter.Reverse))
    return builder.String()
}
```

#### 2.3 KeyedStats

```go
// KeyedStats retrieves stats for a keyed channel.
func (n *Node) KeyedStats(ctx context.Context, ch string) (KeyedStats, error) {
    if n.keyedEngine == nil {
        return KeyedStats{}, ErrorNotAvailable
    }

    if n.config.UseSingleFlight {
        result, err, _ := keyedStatsGroup.Do(ch, func() (any, error) {
            return n.keyedEngine.Stats(ctx, ch)
        })
        if err != nil {
            return KeyedStats{}, err
        }
        return result.(KeyedStats), nil
    }

    return n.keyedEngine.Stats(ctx, ch)
}
```

### 3. Update Config Documentation (config.go)

```go
// UseSingleFlight allows turning on mode where singleflight will be automatically used
// for Node.History (including recovery), Node.Presence/Node.PresenceStats,
// and Node.KeyedSnapshot/Node.KeyedStream/Node.KeyedStats calls.
UseSingleFlight bool
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `node.go` | Add singleflight groups, add KeyedSnapshot/KeyedStream/KeyedStats methods, add result types |
| `config.go` | Update UseSingleFlight documentation |

---

## Key Construction Summary

| Method | Key Components |
|--------|----------------|
| KeyedSnapshot | `ch, cursor, limit, ordered, key, revision` |
| KeyedStream | `ch, since.offset, since.epoch, limit, reverse` |
| KeyedStats | `ch` |

---

## Usage

After implementation:

```go
// Direct engine call (no singleflight)
pubs, pos, cursor, err := node.KeyedEngine().ReadSnapshot(ctx, ch, opts)

// Node method with singleflight (when UseSingleFlight=true)
result, err := node.KeyedSnapshot(ctx, ch, opts)
pubs := result.Publications
pos := result.Position
cursor := result.Cursor
```

---

## Internal Callers to Update

Check if any internal code should use Node methods instead of direct engine calls:

| Location | Current | After |
|----------|---------|-------|
| `client_keyed.go` snapshot phase | `n.keyedEngine.ReadSnapshot()` | Consider `n.KeyedSnapshot()` |
| `client_keyed.go` stream phase | `n.keyedEngine.ReadStream()` | Consider `n.KeyedStream()` |

**Note**: Internal callers that need raw engine access (e.g., for specific error handling) can still use `node.KeyedEngine()` directly.

---

## Testing

1. **Unit test**: Verify singleflight coalesces concurrent identical requests
2. **Unit test**: Verify different options produce different keys (no incorrect coalescing)
3. **Unit test**: Verify `UseSingleFlight=false` bypasses singleflight
4. **Benchmark**: Compare throughput with/without singleflight under concurrent load

---

## Migration Notes

- **Backward compatible**: Existing code using `node.KeyedEngine().ReadSnapshot()` continues to work
- **Opt-in**: Singleflight only active when `UseSingleFlight=true`
- **No CachedKeyedEngine needed**: This approach replaces the need for a caching wrapper

---

## Future Considerations

### Optional: Separate Config Flag

If keyed operations should have independent singleflight control:

```go
type Config struct {
    UseSingleFlight      bool  // History, Presence
    UseKeyedSingleFlight bool  // KeyedSnapshot, KeyedStream (NEW)
}
```

### Optional: Metrics

Add metrics for singleflight effectiveness:

```go
n.metrics.incActionCount("keyed_snapshot", ch)
n.metrics.incActionCount("keyed_snapshot_coalesced", ch)  // When shared result
```
