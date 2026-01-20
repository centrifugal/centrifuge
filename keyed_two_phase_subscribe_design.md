# KeyedEngine: Two-Phase Subscribe Design

## API Changes (Latest)

**Important Update**: The `ReadSnapshot` API now returns `[]*Publication` directly instead of `[]SnapshotEntry`:

```go
// Current API
ReadSnapshot(ctx, ch, opts) ([]*Publication, StreamPosition, cursor string, error)

// What changed:
// - Returns Publications directly (not SnapshotEntry wrappers)
// - Each Publication has Key and Offset already set
// - Epoch is in StreamPosition (not duplicated per entry)
// - No conversion needed - ready to send to clients
```

See **Snapshot Recovery Information** section below for details on what information is available.

## Overview

### Regular Keyed Subscription
```javascript
sub = centrifuge.newSubscription('users', {keyed: true});
sub.subscribe();

// Internally:
// 1. Send subscribe → receive first snapshot page
// 2. Iterate/paginate through snapshot
// 3. Send subscribe with position → switch to streaming
// 4. Fire 'subscribed' event
```

**Note on Presence:** Presence is now implemented as a sub-subscription of the main subscription. See `keyed_presence_subsubscription_design.md` for the complete presence design. Presence also uses the same two-phase subscribe mechanism described in this document.

---

## Protocol Design

### Subscribe Request

```protobuf
message SubscribeRequest {
    string channel = 1;

    // Existing recovery fields
    uint64 recover_since_offset = 2;
    string recover_since_epoch = 3;

    // NEW: Keyed mode flags
    bool keyed = 10;

    // NEW: Subscribe phase
    enum Phase {
        SNAPSHOT = 0;  // Phase 1: request snapshot
        STREAM = 1;    // Phase 2: switch to streaming
    }
    Phase phase = 11;

    // NEW: Snapshot pagination (phase 1 only)
    int32 limit = 12;      // Max entries to return (0 = server default)
    string cursor = 13;    // For pagination

    // NEW: Stream position (phase 2 only)
    uint64 position_offset = 14;
    string position_epoch = 15;

    // NEW: Presence sub-subscription flag
    bool presence = 20;    // Indicates this is a presence sub-subscription
}
```

### Subscribe Result

```protobuf
message SubscribeResult {
    // Existing fields
    bool recovered = 7;

    // NEW: Stream position
    uint64 offset = 4;
    string epoch = 5;

    // NEW: Snapshot data (phase 1)
    repeated Publication publications = 6;  // Snapshot entries

    // NEW: Keyed mode indicators
    bool keyed = 10;
    string cursor = 11;                      // Next page cursor (empty if done)

    // NEW: Subscribe phase result
    enum Phase {
        SNAPSHOT = 0;   // Still in snapshot phase (more pages available)
        STREAMING = 1;  // Now streaming real-time updates
    }
    Phase phase = 12;

    // NEW: Presence capability indicator
    bool presence_available = 20;  // Server indicates presence is available for this channel
}
```

---

## Two-Phase Subscribe Flow

### Phase 1: Snapshot Iteration

```
Client                              Server (KeyedEngine)
  |                                        |
  | SubscribeRequest                       |
  |   channel: "users"                     |
  |   keyed: true                          |
  |   phase: SNAPSHOT                      |
  |   limit: 100                           |
  |   cursor: ""                           |
  |--------------------------------------->|
  |                                        |
  |                    SubscribeResult     |
  |       keyed: true                      |
  |       phase: SNAPSHOT                  |
  |       publications: [100 entries]      |
  |       cursor: "page2_token"            |
  |       offset: 5000                     |
  |       epoch: "abc"                     |
  |<---------------------------------------|
  |                                        |
  | (Client processes 100 entries)         |
  |                                        |
  | SubscribeRequest                       |
  |   channel: "users"                     |
  |   keyed: true                          |
  |   phase: SNAPSHOT                      |
  |   limit: 100                           |
  |   cursor: "page2_token"                |
  |--------------------------------------->|
  |                                        |
  |                    SubscribeResult     |
  |       publications: [100 entries]      |
  |       cursor: "page3_token"            |
  |<---------------------------------------|
  |                                        |
  | (Continue until cursor is empty)       |
  |                                        |
  | SubscribeRequest                       |
  |   cursor: "pageN_token"                |
  |--------------------------------------->|
  |                                        |
  |                    SubscribeResult     |
  |       publications: [50 entries]       |
  |       cursor: ""  ← Last page          |
  |       offset: 5000                     |
  |       epoch: "abc"                     |
  |<---------------------------------------|
  |                                        |
```

### Phase 2: Switch to Streaming

```
Client                              Server (KeyedEngine)
  |                                        |
  | (Snapshot complete, switch to stream)  |
  |                                        |
  | SubscribeRequest                       |
  |   channel: "users"                     |
  |   keyed: true                          |
  |   phase: STREAM                        |
  |   position_offset: 5000                |
  |   position_epoch: "abc"                |
  |--------------------------------------->|
  |                                        |
  |                    SubscribeResult     |
  |       phase: STREAMING                 |
  |       offset: 5000                     |
  |       epoch: "abc"                     |
  |<---------------------------------------|
  |                                        |
  | (Fire 'subscribed' event)              |
  |                                        |
  | (Now receive real-time Push messages)  |
  |                                        |
  |                         Push           |
  |       pub: {key: "user:123", ...}      |
  |<---------------------------------------|
  |                                        |
```

---

## Server-Side Implementation

### Subscription Handler

```go
func (c *Client) handleSubscribe(req *SubscribeRequest) (*SubscribeResult, error) {
    channelOpts := c.node.channelOpts(req.Channel)

    if !channelOpts.Keyed {
        // Regular subscription (existing code)
        return c.handleRegularSubscribe(req)
    }

    // Keyed subscription - two-phase process
    if req.Phase == SubscribeRequest_SNAPSHOT {
        // Phase 1: Return snapshot page
        return c.handleSnapshotPhase(req)
    } else {
        // Phase 2: Switch to streaming
        return c.handleStreamPhase(req)
    }
}

func (c *Client) handleSnapshotPhase(req *SubscribeRequest) (*SubscribeResult, error) {
    // Build options for ReadSnapshot
    opts := centrifuge.KeyedReadSnapshotOptions{
        Limit:  int(req.Limit),
        Cursor: req.Cursor,
    }

    // If client provided position from previous page, pass it for epoch validation
    if req.PositionOffset > 0 || req.PositionEpoch != "" {
        opts.SnapshotRevision = &centrifuge.StreamPosition{
            Offset: req.PositionOffset,
            Epoch:  req.PositionEpoch,
        }
    }

    // Read snapshot page from KeyedEngine
    // Returns []*Publication directly - each with Key and Offset already set
    // Server validates epoch internally and returns ErrorUnrecoverablePosition if it changed
    pubs, streamPos, cursor, err := c.node.keyedEngine.ReadSnapshot(
        c.ctx,
        req.Channel,
        opts,
    )
    if err != nil {
        // If ErrorUnrecoverablePosition, client should restart from beginning
        return nil, err
    }

    // No conversion needed - Publications are ready to send
    // Each pub has:
    //   - pub.Key: the entry key
    //   - pub.Offset: revision offset (compare with streamPos.Offset)
    //   - pub.Data: the actual data
    //   - pub.Tags: any metadata (including score for ordered snapshots)
    //   - pub.Info: optional ClientInfo
    //   - pub.Removed: false (true only for unpublish/remove events)
    //
    // streamPos.Epoch: common epoch for all entries in this snapshot
    // Client should verify: pub.Offset <= streamPos.Offset

    // Determine phase
    phase := SubscribeResult_SNAPSHOT
    if cursor == "" {
        // Last page - but still in snapshot phase
        // Client will send STREAM subscribe next
    }

    return &SubscribeResult{
        Keyed:        true,
        Phase:        phase,
        Publications: pubs,
        Cursor:       cursor,
        Offset:       streamPos.Offset,
        Epoch:        streamPos.Epoch,
    }, nil
}

func (c *Client) handleStreamPhase(req *SubscribeRequest) (*SubscribeResult, error) {
    // Subscribe to real-time updates
    err := c.node.keyedEngine.Subscribe(req.Channel)
    if err != nil {
        return nil, err
    }

    // Add to client's subscriptions
    c.mu.Lock()
    c.subscriptions[req.Channel] = &subscription{
        channel: req.Channel,
        keyed:   true,
        offset:  req.PositionOffset,
        epoch:   req.PositionEpoch,
    }
    c.mu.Unlock()

    return &SubscribeResult{
        Keyed:  true,
        Phase:  SubscribeResult_STREAMING,
        Offset: req.PositionOffset,
        Epoch:  req.PositionEpoch,
    }, nil
}
```

---

## Client-Side Implementation (JavaScript)

```javascript
class Subscription {
    constructor(centrifuge, channel, opts) {
        this._centrifuge = centrifuge;
        this._channel = channel;
        this._keyed = opts.keyed || false;
        this._state = 'unsubscribed';

        // For keyed subscriptions
        this._snapshot = [];  // Accumulated snapshot
        this._streamPosition = null;

        // Presence sub-subscription (see keyed_presence_subsubscription_design.md)
        this._presence = new PresenceSubscription(this);
    }

    get presence() {
        return this._presence;
    }

    async subscribe() {
        if (this._keyed) {
            await this._keyedSubscribe();
        } else {
            await this._regularSubscribe();
        }
    }

    async _keyedSubscribe() {
        this._setState('subscribing');

        try {
            // Phase 1: Iterate snapshot
            await this._fetchSnapshot();

            // Phase 2: Switch to streaming
            await this._switchToStreaming();

            this._setState('subscribed');
            this._emit('subscribed', {
                channel: this._channel,
                keyed: true,
                snapshot: this._snapshot,
                offset: this._streamPosition.offset,
                epoch: this._streamPosition.epoch
            });
        } catch (err) {
            this._setState('unsubscribed');
            this._emit('error', err);
        }
    }

    async _fetchSnapshot() {
        let cursor = '';
        this._snapshot = [];
        let position = null;  // Will be set from first response

        while (true) {
            try {
                const result = await this._centrifuge._sendSubscribe({
                    channel: this._channel,
                    keyed: true,
                    phase: 'snapshot',
                    limit: 100,
                    cursor: cursor,
                    // Pass position for server-side epoch validation
                    position_offset: position?.offset || 0,
                    position_epoch: position?.epoch || ''
                });

                // First response: capture position with epoch!
                if (position === null) {
                    position = {
                        offset: result.offset,
                        epoch: result.epoch
                    };
                }

                // Accumulate snapshot entries
                this._snapshot.push(...result.publications);

                // Emit progress event
                this._emit('snapshot_progress', {
                    entries: result.publications,
                    total: this._snapshot.length,
                    cursor: result.cursor
                });

                // Update stream position (offset may have advanced)
                this._streamPosition = {
                    offset: result.offset,
                    epoch: result.epoch
                };

                // Check if done
                if (result.cursor === '') {
                    break;
                }

                cursor = result.cursor;
            } catch (err) {
                // If ErrorUnrecoverablePosition, restart from beginning
                if (err.code === 112) {  // ErrorUnrecoverablePosition
                    console.log('Epoch changed, restarting snapshot');
                    cursor = '';
                    this._snapshot = [];
                    position = null;
                    continue;
                }
                throw err;
            }
        }

        console.log(`Snapshot complete: ${this._snapshot.length} entries`);
    }

    async _switchToStreaming() {
        const result = await this._centrifuge._sendSubscribe({
            channel: this._channel,
            keyed: true,
            phase: 'stream',
            position_offset: this._streamPosition.offset,
            position_epoch: this._streamPosition.epoch
        });

        if (result.phase !== 'streaming') {
            throw new Error('Failed to switch to streaming');
        }

        console.log('Now streaming:', this._channel);
    }

    _handlePublication(pub) {
        if (this._keyed) {
            if (pub.removed) {
                this._emit('remove', {
                    key: pub.key,
                    offset: pub.offset
                });

                // Update local snapshot
                this._snapshot = this._snapshot.filter(e => e.key !== pub.key);
            } else {
                this._emit('update', {
                    key: pub.key,
                    data: pub.data,
                    score: pub.tags?.score,
                    offset: pub.offset
                });

                // Update local snapshot
                const idx = this._snapshot.findIndex(e => e.key === pub.key);
                if (idx >= 0) {
                    this._snapshot[idx] = pub;
                } else {
                    this._snapshot.push(pub);
                }
            }
        } else {
            this._emit('publication', {data: pub.data});
        }
    }
}
```

---

## Complete Flow Examples

### Example 1: Regular Keyed Subscription

```javascript
const users = centrifuge.newSubscription('users', {keyed: true});

users.on('snapshot_progress', (ctx) => {
    console.log(`Loading... ${ctx.total} entries so far`);
});

users.on('subscribed', (ctx) => {
    console.log(`Subscribed! Total: ${ctx.snapshot.length} entries`);
    ctx.snapshot.forEach(entry => {
        console.log(entry.key, entry.data);
    });
});

users.on('update', (ctx) => {
    console.log('Updated:', ctx.key, ctx.data);
});

users.on('remove', (ctx) => {
    console.log('Removed:', ctx.key);
});

users.subscribe();

// Flow:
// 1. subscribing → snapshot_progress (100 entries)
// 2. snapshot_progress (200 entries)
// 3. snapshot_progress (250 entries, done)
// 4. subscribed (snapshot: 250 entries)
// 5. update/remove events for real-time changes
```

---

## Snapshot Recovery Information

### What's Available for Recovery

Each snapshot page provides all information needed for recovery:

#### Per-Entry Information (in each Publication)
- **`pub.Key`** - The entry key (e.g., "user:123")
- **`pub.Offset`** - Individual entry revision offset
- **`pub.Data`** - The actual payload
- **`pub.Tags`** - Metadata (may include score for ordered snapshots)
- **`pub.Info`** - Optional ClientInfo (who published)
- **`pub.Removed`** - Always false in snapshots (true only in remove events)

#### Snapshot-Level Information (in SubscribeResult)
- **`streamPos.Offset`** - Current stream position (top offset)
- **`streamPos.Epoch`** - Stream epoch (changes on history reset)
- **`cursor`** - Pagination cursor (empty "" means last page)

### Client-Side Recovery Logic

Clients should validate each entry:
```javascript
for (const pub of result.publications) {
    // Verify entry is part of this snapshot
    if (pub.offset <= result.offset) {
        // Valid entry - offset is at or before snapshot position
        processEntry(pub);
    } else {
        // Should not happen - entry is newer than snapshot position
        console.error('Invalid entry offset');
    }
}

// Store snapshot position for phase 2
this._streamPosition = {
    offset: result.offset,
    epoch: result.epoch
};
```

### Epoch Handling

The epoch is **snapshot-level**, not per-entry:
- All entries in a snapshot share the same epoch
- Epoch is returned in `streamPos.Epoch` (not duplicated per entry)
- Server validates epoch internally on each pagination request
- If epoch changes, server returns `ErrorUnrecoverablePosition` error
- Client catches the error and restarts snapshot iteration from beginning

This design avoids duplicating epoch in every Publication (saves bandwidth) and simplifies client logic - client just passes the StreamPosition it received, server handles validation.

### Ordered Snapshots and Scores

For ordered snapshots (sorted by score):
- Publications are returned in score order (descending)
- Scores can be included in `pub.Tags` (e.g., `Tags: {"score": "100"}`)
- Alternatively, clients can rely on the order of entries in the response
- Score handling depends on application needs:
  - If clients need to re-sort locally → include score in Tags during Publish
  - If clients only need to display in order → order is preserved in response

Example publishing with score in Tags:
```go
engine.Publish(ctx, "leaderboard", "user:123", data, KeyedPublishOptions{
    Tags:    map[string]string{"score": "1500"},
    Score:   1500,  // For ordering in snapshot
    Ordered: true,
})
```

---

## Benefits

1. **Client controls pagination** - Can show progress, cancel, etc.
2. **Large snapshots handled gracefully** - No single huge message
3. **Server stateless** - Each snapshot request is independent
4. **Clear lifecycle** - subscribing → snapshot_progress → subscribed
5. **Minimal protocol changes** - Reuse Publication, add phase/cursor fields
6. **Flexible** - Client can customize snapshot page size
7. **Clean separation** - Snapshot phase vs streaming phase explicit
8. **Backward compatible** - Regular subscriptions unchanged
9. **Reusable for presence** - Presence sub-subscriptions use the same two-phase mechanism

---

## Protocol Summary

### New Fields

**SubscribeRequest:**
- `bool keyed` - Indicates keyed subscription
- `Phase phase` (SNAPSHOT | STREAM) - Subscribe phase
- `int32 limit` - Pagination limit for snapshot
- `string cursor` - Pagination cursor
- `uint64 position_offset` - Stream position offset
- `string position_epoch` - Stream position epoch
- `bool presence` - Indicates presence sub-subscription (see keyed_presence_subsubscription_design.md)

**SubscribeResult:**
- `bool keyed` - Server confirms keyed mode
- `Phase phase` (SNAPSHOT | STREAMING) - Current phase
- `repeated Publication publications` - Snapshot entries
- `string cursor` - Next page cursor (empty if done)
- `uint64 offset` - Stream position offset
- `string epoch` - Stream position epoch
- `bool presence_available` - Indicates presence is available for this channel

**No changes to:**
- Publication (already has key, removed, tags)
- Push (already has pub)

This design gives clients full control over snapshot iteration while keeping the protocol simple and the server stateless!

---

## Complete End-to-End Example

### Scenario: Client with NO initial state subscribing to "users" channel

The client starts with:
- No StreamPosition (no offset, no epoch)
- No cursor
- Never subscribed before

Let's trace through what actually happens:

### Step 1: First ReadSnapshot Call

**Client sends:**
```go
pubs, streamPos, cursor, err := engine.ReadSnapshot(ctx, "users", KeyedReadSnapshotOptions{
    Limit:            100,           // Get 100 entries per page
    Cursor:           "",             // First page
    SnapshotRevision: nil,            // No position yet - first time!
    Ordered:          false,
})
```

**What server does (Memory Engine):**
```go
// In getSnapshot():
channel, ok := h.channels["users"]
if !ok {
    // Channel doesn't exist yet - create stream and return empty
    return nil, h.createStreamPosition("users"), "", nil
    // createStreamPosition creates NEW stream and returns:
    //   StreamPosition{Offset: 0, Epoch: "xyz_generated_epoch"}
}

// If channel exists:
var streamPosition StreamPosition
if channel.stream != nil {
    streamPosition = StreamPosition{
        Offset: channel.stream.Top(),      // e.g., 5000
        Epoch:  channel.stream.Epoch(),    // e.g., "abc123"
    }
}

// SnapshotRevision is nil on first request, so skip epoch check
if opts.SnapshotRevision != nil {
    if streamPosition.Epoch != opts.SnapshotRevision.Epoch {
        // Epoch changed! Return error
        return nil, streamPosition, "", ErrorUnrecoverablePosition
    }
}

// Build publications and return
return pubs, streamPosition, cursor, nil
```

**Server returns:**
```go
pubs = []*Publication{
    {Key: "user:1", Offset: 100, Data: []byte("alice"), ...},
    {Key: "user:2", Offset: 150, Data: []byte("bob"), ...},
    // ... 98 more entries
}
streamPos = StreamPosition{Offset: 5000, Epoch: "abc123"}
cursor = "page2_cursor"
err = nil
```

**KEY POINT**: The client gets the epoch from `streamPos.Epoch` in the FIRST response! The server ALWAYS returns a StreamPosition with both Offset and Epoch, even on the first call.

### Step 2: Second ReadSnapshot Call (pagination)

**Client sends:**
```go
pubs, streamPos, cursor, err := engine.ReadSnapshot(ctx, "users", KeyedReadSnapshotOptions{
    Limit:            100,
    Cursor:           "page2_cursor",  // From previous response
    SnapshotRevision: &StreamPosition{ // Now we have position from step 1!
        Offset: 5000,
        Epoch:  "abc123",
    },
    Ordered:          false,
})
```

**What server does:**
```go
// Get channel and current stream position
streamPosition = StreamPosition{
    Offset: channel.stream.Top(),    // Still 5000 (or maybe 5001 if new data)
    Epoch:  channel.stream.Epoch(),  // Still "abc123"
}

// THIS TIME check epoch since client provided SnapshotRevision
if opts.SnapshotRevision != nil {
    if streamPosition.Epoch != opts.SnapshotRevision.Epoch {
        // Epoch changed! Return ErrorUnrecoverablePosition
        return nil, streamPosition, "", ErrorUnrecoverablePosition
    }
}

// Epoch matches, continue with pagination using cursor
return pubs, streamPosition, newCursor, nil
```

**Server returns:**
```go
pubs = []*Publication{
    {Key: "user:101", Offset: 200, Data: []byte("charlie"), ...},
    // ... 99 more entries
}
streamPos = StreamPosition{Offset: 5000, Epoch: "abc123"}  // Same epoch!
cursor = "page3_cursor"
```

### Step 3: Last Page

**Client sends:**
```go
pubs, streamPos, cursor, err := engine.ReadSnapshot(ctx, "users", KeyedReadSnapshotOptions{
    Limit:            100,
    Cursor:           "page3_cursor",
    SnapshotRevision: &StreamPosition{Offset: 5000, Epoch: "abc123"},
})
```

**Server returns:**
```go
pubs = []*Publication{
    {Key: "user:201", Offset: 300, Data: []byte("diana"), ...},
    // ... 49 more entries (only 50 left)
}
streamPos = StreamPosition{Offset: 5000, Epoch: "abc123"}
cursor = ""  // ← EMPTY = Last page!
```

### Step 4: Client Switches to Streaming

Now the client has:
- Complete snapshot (250 entries accumulated)
- StreamPosition{Offset: 5000, Epoch: "abc123"}
- Ready to receive real-time updates

**Client subscribes to channel for streaming:**
```go
// Subscribe to channel to receive real-time Push messages
err := engine.Subscribe("users")

// Client stores position for recovery:
subscription.offset = 5000
subscription.epoch = "abc123"

// Fire 'subscribed' event to application
emit('subscribed', {
    channel: "users",
    snapshot: allAccumulatedPubs,  // 250 entries
    offset: 5000,
    epoch: "abc123"
})
```

**From now on, client receives Push messages:**
```go
Push{
    Pub: &Publication{
        Key:    "user:999",
        Offset: 5001,  // Next offset after snapshot
        Data:   []byte("new user"),
        Removed: false,
    }
}
```

### Summary: How Epoch Validation Works

| Step | SnapshotRevision (input) | StreamPosition (output) | Server Action |
|------|-------------------------|------------------------|---------------|
| 1    | `nil` (first call)      | `{Offset: 5000, Epoch: "abc123"}` | Returns epoch, skips validation |
| 2    | `{5000, "abc123"}`      | `{Offset: 5000, Epoch: "abc123"}` | Validates epoch matches |
| 3    | `{5000, "abc123"}`      | `{Offset: 5000, Epoch: "abc123"}` | Validates epoch matches |
| 4    | Uses `{5000, "abc123"}` for streaming | - | Switch to streaming |

**Client flow:**
1. First request: Client passes `nil`, gets epoch from response
2. Subsequent requests: Client passes received `StreamPosition`
3. Server validates epoch internally
4. If epoch changed: Server returns `ErrorUnrecoverablePosition`, client restarts

### Verification: Implementation Returns Epoch

✅ **Memory Engine** (`keyed_engine_memory.go:735-738`):
```go
if channel.stream != nil {
    streamPosition = StreamPosition{
        Offset: channel.stream.Top(),
        Epoch:  channel.stream.Epoch(),  // ← Always included
    }
}
```

✅ **Redis Engine** (`keyed_engine_redis.go:812`):
```go
streamPos := StreamPosition{Offset: streamOffset, Epoch: streamEpoch}  // ← Always included
```

✅ **Empty Channel** (`keyed_engine_memory.go:836-839`):
```go
return StreamPosition{
    Offset: 0,
    Epoch:  stream.Epoch(),  // ← Even new channels have epoch
}
```

### Complete Client Implementation Example

```javascript
class KeyedSubscription {
    async subscribe() {
        // Step 1-3: Fetch complete snapshot
        const {snapshot, position} = await this._fetchCompleteSnapshot();

        // position.epoch came from FIRST ReadSnapshot response!
        console.log(`Snapshot: ${snapshot.length} entries`);
        console.log(`Position: offset=${position.offset}, epoch=${position.epoch}`);

        // Step 4: Switch to streaming
        await this._subscribeForStreaming(position);

        // Done!
        this.emit('subscribed', {snapshot, position});
    }

    async _fetchCompleteSnapshot() {
        let allPubs = [];
        let cursor = "";
        let position = null;  // Will be set from first response

        while (true) {
            try {
                const result = await this._rpc.readSnapshot({
                    channel: this._channel,
                    limit: 100,
                    cursor: cursor,
                    snapshotRevision: position,  // nil first time, then set for validation
                });

                // First response: capture position with epoch!
                if (position === null) {
                    position = {
                        offset: result.streamPosition.offset,
                        epoch: result.streamPosition.epoch  // ← GOT IT HERE!
                    };
                }

                // Accumulate publications
                allPubs.push(...result.publications);

                // Check if done
                if (result.cursor === "") {
                    break;  // Last page
                }

                cursor = result.cursor;
            } catch (err) {
                // Server returns ErrorUnrecoverablePosition if epoch changed
                if (err.code === 112) {  // ErrorUnrecoverablePosition
                    console.warn('Epoch changed, restarting snapshot');
                    // Restart from beginning
                    allPubs = [];
                    cursor = "";
                    position = null;
                    continue;
                }
                throw err;
            }
        }

        return {snapshot: allPubs, position};
    }

    async _subscribeForStreaming(position) {
        await this._rpc.subscribe({
            channel: this._channel,
            positionOffset: position.offset,
            positionEpoch: position.epoch  // Use epoch from snapshot
        });
    }
}
```

This design ensures:
1. ✅ Client ALWAYS gets epoch in first ReadSnapshot response
2. ✅ Client just passes StreamPosition - server validates epoch internally
3. ✅ Server returns ErrorUnrecoverablePosition if epoch changed
4. ✅ Client catches error and restarts automatically
5. ✅ Client uses epoch to switch to streaming phase
6. ✅ Server returns epoch even for empty/new channels
7. ✅ Both Memory and Redis engines implement this correctly

**Key benefit:** Simplified client logic - no manual epoch comparison needed!
