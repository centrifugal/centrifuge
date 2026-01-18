# KeyedEngine: Final Client API Design

## Core Principle

**Reuse existing events completely.** No new events like `update`/`remove` - everything through `publication`.

---

## Client API

### Regular Subscription (Unchanged)

```javascript
const chat = centrifuge.newSubscription('chat:lobby');

chat.on('subscribed', (ctx) => {
    // ctx.recovered - true if recovered from previous position
    console.log('Subscribed, recovered:', ctx.recovered);
});

chat.on('publication', (ctx) => {
    console.log('Message:', ctx.data);
});

chat.subscribe();
```

### Keyed Subscription

```javascript
const users = centrifuge.newSubscription('users', {keyed: true});

users.on('subscribed', (ctx) => {
    // ctx.publications - full snapshot (array of Publications)
    // ctx.recovered - true if snapshot + stream recovery succeeded
    console.log('Snapshot:', ctx.publications);
    console.log('Total entries:', ctx.publications.length);
    console.log('Recovered:', ctx.recovered);

    // User iterates snapshot
    ctx.publications.forEach(pub => {
        console.log(pub.key, pub.data, pub.tags);
    });
});

users.on('publication', (ctx) => {
    // ALL updates come through publication event
    // User checks ctx.data.key and ctx.data.removed

    if (ctx.data.key) {
        // This is a keyed publication
        if (ctx.data.removed) {
            console.log('Removed:', ctx.data.key);
        } else {
            console.log('Updated:', ctx.data.key, ctx.data.data);
        }
    } else {
        // Regular publication (shouldn't happen in keyed channel)
        console.log('Message:', ctx.data.data);
    }
});

users.subscribe();
```

### Keyed Presence

```javascript
const presence = centrifuge.newSubscription('chat:lobby', {
    keyed: true,
    presence: true
});

presence.on('subscribed', (ctx) => {
    // ctx.publications - full presence snapshot
    console.log('Online users:', ctx.publications.map(pub => {
        return {
            clientID: pub.key,
            userID: JSON.parse(pub.data).userID
        };
    }));
});

presence.on('publication', (ctx) => {
    // Presence updates
    if (ctx.data.removed) {
        console.log('User left:', ctx.data.key);
    } else {
        const info = JSON.parse(ctx.data.data);
        console.log('User joined:', info.userID);
    }
});

presence.subscribe();
```

---

## Event Context Structure

### subscribed event (Regular)

```javascript
{
    channel: "chat:lobby",
    recovered: false,
    // No publications array for regular subscriptions
}
```

### subscribed event (Keyed)

```javascript
{
    channel: "users",
    recovered: false,
    publications: [  // ← Full snapshot
        {
            key: "user:123",
            data: {...},
            offset: 100,
            tags: {score: "95"}
        },
        {
            key: "user:456",
            data: {...},
            offset: 99,
            tags: {score: "90"}
        }
        // ... all entries
    ],
    offset: 5000,
    epoch: "abc"
}
```

### publication event (Regular)

```javascript
{
    channel: "chat:lobby",
    data: {
        data: "hello",      // ← Message data
        offset: 42
    }
}
```

### publication event (Keyed)

```javascript
// Update
{
    channel: "users",
    data: {
        key: "user:123",    // ← Key present
        data: {...},
        offset: 5001,
        removed: false,     // ← Not removed
        tags: {score: "96"}
    }
}

// Remove
{
    channel: "users",
    data: {
        key: "user:123",
        offset: 5002,
        removed: true       // ← Removed
    }
}
```

---

## Two-Phase Subscribe (Internal to SDK)

User doesn't see the two phases - SDK handles it transparently.

### SDK Implementation

```javascript
class Subscription {
    constructor(centrifuge, channel, opts) {
        this._centrifuge = centrifuge;
        this._channel = channel;
        this._keyed = opts.keyed || false;
        this._presence = opts.presence || false;
        this._state = 'unsubscribed';

        // Transform presence to __presence: channel
        if (this._presence) {
            this._actualChannel = `__presence:${channel}`;
            this._keyed = true;
        } else {
            this._actualChannel = channel;
        }
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
            // Phase 1: Fetch complete snapshot
            const snapshot = await this._fetchCompleteSnapshot();

            // Phase 2: Switch to streaming
            const streamResult = await this._switchToStreaming(
                snapshot.offset,
                snapshot.epoch
            );

            // Fire subscribed event with full snapshot
            this._setState('subscribed');
            this._emit('subscribed', {
                channel: this._channel,  // Original channel (not __presence:)
                recovered: false,        // TODO: handle recovery
                publications: snapshot.publications,
                offset: snapshot.offset,
                epoch: snapshot.epoch
            });

        } catch (err) {
            this._setState('unsubscribed');
            this._emit('error', err);
        }
    }

    async _fetchCompleteSnapshot() {
        let cursor = '';
        let allPublications = [];
        let finalOffset = 0;
        let finalEpoch = '';

        while (true) {
            const result = await this._centrifuge._sendSubscribe({
                channel: this._actualChannel,
                keyed: true,
                phase: 'snapshot',
                limit: 100,
                cursor: cursor
            });

            // Accumulate publications
            allPublications.push(...result.publications);

            // Store position
            finalOffset = result.offset;
            finalEpoch = result.epoch;

            // Done?
            if (result.cursor === '') {
                break;
            }

            cursor = result.cursor;
        }

        return {
            publications: allPublications,
            offset: finalOffset,
            epoch: finalEpoch
        };
    }

    async _switchToStreaming(offset, epoch) {
        return await this._centrifuge._sendSubscribe({
            channel: this._actualChannel,
            keyed: true,
            phase: 'stream',
            position_offset: offset,
            position_epoch: epoch
        });
    }

    _handlePublication(pub) {
        // Just forward publication to handler
        // User checks pub.key and pub.removed themselves
        this._emit('publication', {
            channel: this._channel,
            data: pub
        });
    }
}
```

---

## Recovery for Keyed Subscriptions

When client reconnects with stored position:

```javascript
class Subscription {
    async _keyedSubscribeWithRecovery(storedOffset, storedEpoch) {
        this._setState('subscribing');

        try {
            // Try to recover from stored position
            const result = await this._centrifuge._sendSubscribe({
                channel: this._actualChannel,
                keyed: true,
                phase: 'stream',
                position_offset: storedOffset,
                position_epoch: storedEpoch
            });

            if (result.recovered) {
                // Recovery succeeded!
                // Server sent missed publications
                const snapshot = this._reconstructSnapshot(result.publications);

                this._emit('subscribed', {
                    channel: this._channel,
                    recovered: true,          // ← Recovered flag
                    publications: snapshot,
                    offset: result.offset,
                    epoch: result.epoch
                });

            } else {
                // Recovery failed (epoch mismatch, too far behind, etc.)
                // Fall back to full snapshot fetch
                await this._keyedSubscribe();
            }

        } catch (err) {
            // Recovery failed, fall back to full snapshot
            await this._keyedSubscribe();
        }
    }

    _reconstructSnapshot(publications) {
        // Rebuild snapshot from missed publications
        const snapshot = new Map(this._lastSnapshot.map(p => [p.key, p]));

        publications.forEach(pub => {
            if (pub.removed) {
                snapshot.delete(pub.key);
            } else {
                snapshot.set(pub.key, pub);
            }
        });

        return Array.from(snapshot.values());
    }
}
```

---

## Server-Side Presence Handling

```go
func (c *Client) handleSubscribe(req *SubscribeRequest) (*SubscribeResult, error) {
    // Detect presence channel
    isPresence := strings.HasPrefix(req.Channel, "__presence:")

    if !c.channelOpts(req.Channel).Keyed {
        // Regular subscription
        return c.handleRegularSubscribe(req)
    }

    // Keyed subscription
    var result *SubscribeResult
    var err error

    if req.Phase == SubscribeRequest_SNAPSHOT {
        result, err = c.handleSnapshotPhase(req)
    } else {
        result, err = c.handleStreamPhase(req)
    }

    if err != nil {
        return nil, err
    }

    // If this is a presence subscription and we just switched to streaming,
    // start maintaining client presence
    if isPresence && req.Phase == SubscribeRequest_STREAM {
        c.startPresenceMaintenance(req.Channel)
    }

    return result, nil
}

func (c *Client) startPresenceMaintenance(presenceChannel string) {
    // Add this client to presence
    presenceInfo := &ClientInfo{
        ClientID: c.ID(),
        UserID:   c.UserID(),
        ConnInfo: c.ConnInfo(),
        ChanInfo: c.ChanInfo(),
    }

    data, _ := json.Marshal(presenceInfo)

    // Initial publish
    c.node.KeyedPublish(c.ctx, presenceChannel, c.ID(), data)

    // Start keepalive goroutine
    go func() {
        ticker := time.NewTicker(25 * time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ticker.C:
                // Update presence
                c.node.KeyedPublish(c.ctx, presenceChannel, c.ID(), data)

            case <-c.ctx.Done():
                // Remove presence on disconnect
                c.node.KeyedRemove(c.ctx, presenceChannel, c.ID())
                return
            }
        }
    }()
}
```

---

## Complete Examples

### Example 1: Leaderboard

```javascript
const leaderboard = centrifuge.newSubscription('leaderboard', {keyed: true});

leaderboard.on('subscribed', (ctx) => {
    // ctx.publications already sorted by score (server-side)
    console.log('Top 10 players:');
    ctx.publications.slice(0, 10).forEach((pub, i) => {
        console.log(`${i+1}. ${pub.key} - ${pub.tags.score} points`);
    });
});

leaderboard.on('publication', (ctx) => {
    const pub = ctx.data;

    if (pub.key) {
        if (pub.removed) {
            console.log(`Player ${pub.key} removed from leaderboard`);
        } else {
            console.log(`Player ${pub.key} now has ${pub.tags.score} points`);
        }
    }
});

leaderboard.subscribe();
```

### Example 2: Online Presence

```javascript
const presence = centrifuge.newSubscription('chat:lobby', {
    keyed: true,
    presence: true
});

let onlineUsers = new Map();

presence.on('subscribed', (ctx) => {
    // Build initial online users map
    ctx.publications.forEach(pub => {
        const info = JSON.parse(pub.data);
        onlineUsers.set(pub.key, info);
    });

    console.log(`${onlineUsers.size} users online`);
    updateUserList(onlineUsers);
});

presence.on('publication', (ctx) => {
    const pub = ctx.data;

    if (pub.removed) {
        onlineUsers.delete(pub.key);
        console.log('User left:', pub.key);
    } else {
        const info = JSON.parse(pub.data);
        onlineUsers.set(pub.key, info);
        console.log('User joined:', info.userID);
    }

    updateUserList(onlineUsers);
});

presence.subscribe();
```

### Example 3: Document Collaboration (Snapshot)

```javascript
const doc = centrifuge.newSubscription('doc:123', {keyed: true});

let documentState = new Map();

doc.on('subscribed', (ctx) => {
    // Initialize document from snapshot
    ctx.publications.forEach(pub => {
        documentState.set(pub.key, JSON.parse(pub.data));
    });

    console.log('Document loaded:', documentState.size, 'fields');
    renderDocument(documentState);
});

doc.on('publication', (ctx) => {
    const pub = ctx.data;

    if (pub.removed) {
        documentState.delete(pub.key);
    } else {
        documentState.set(pub.key, JSON.parse(pub.data));
    }

    renderDocument(documentState);
});

doc.subscribe();
```

---

## Summary

**Events:**
- ✅ `subscribed` - fires after snapshot loaded + streaming started
  - Has `publications` array (full snapshot) for keyed subscriptions
  - Has `recovered` flag (true if recovered from stored position)
- ✅ `publication` - all updates (user checks `pub.key` and `pub.removed`)
- ✅ No new events needed!

**API:**
```javascript
// Regular subscription (unchanged)
centrifuge.newSubscription('channel')

// Keyed subscription
centrifuge.newSubscription('channel', {keyed: true})

// Keyed presence
centrifuge.newSubscription('channel', {keyed: true, presence: true})
```

**Two-phase subscribe:**
- ✅ Transparent to user (SDK handles internally)
- ✅ User just calls `subscribe()` once
- ✅ `subscribed` event fires when complete

**Presence:**
- ✅ User subscribes with `{keyed: true, presence: true}`
- ✅ SDK internally uses `__presence:channel`
- ✅ Server automatically maintains client presence
- ✅ Same events as regular keyed subscription

Simple, clean, and consistent with existing Centrifuge API!
