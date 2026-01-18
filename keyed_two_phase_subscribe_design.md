# KeyedEngine: Two-Phase Subscribe Design

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
    // Read snapshot page from KeyedEngine
    entries, streamPos, cursor, err := c.node.keyedEngine.ReadSnapshot(
        c.ctx,
        req.Channel,
        centrifuge.ReadSnapshotOptions{
            Limit:  int(req.Limit),
            Cursor: req.Cursor,
        },
    )
    if err != nil {
        return nil, err
    }

    // Convert to Publications
    pubs := make([]*Publication, len(entries))
    for i, entry := range entries {
        pubs[i] = &Publication{
            Key:    entry.Key,
            Data:   entry.Data,
            Offset: entry.Offset,
            Tags:   map[string]string{"score": fmt.Sprint(entry.Score)},
        }
    }

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

        while (true) {
            const result = await this._centrifuge._sendSubscribe({
                channel: this._channel,
                keyed: true,
                phase: 'snapshot',
                limit: 100,
                cursor: cursor
            });

            // Accumulate snapshot entries
            this._snapshot.push(...result.publications);

            // Emit progress event
            this._emit('snapshot_progress', {
                entries: result.publications,
                total: this._snapshot.length,
                cursor: result.cursor
            });

            // Store stream position
            this._streamPosition = {
                offset: result.offset,
                epoch: result.epoch
            };

            // Check if done
            if (result.cursor === '') {
                break;
            }

            cursor = result.cursor;
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
