# KeyedEngine Protocol Design (Using Existing Publication Type)

## Key Insight

**We don't need new protocol types!** The existing `Publication` struct already has everything we need:

```go
type Publication struct {
    Offset  uint64              // Stream position
    Data    []byte              // Entry data
    Tags    map[string]string   // Metadata (score, etc.)
    Key     string              // Entry key ← Already exists!
    Removed bool                // Add/update vs remove ← Already exists!
    // ... other fields
}
```

## Protocol Design

### 1. Subscribe Request (No Changes Needed)

```protobuf
message SubscribeRequest {
    string channel = 1;
    // Existing fields for recovery/positioning
    uint64 recover_since_offset = 2;
    string recover_since_epoch = 3;
}
```

Client learns if channel is keyed from **server response**.

### 2. Subscribe Result (Minimal Addition)

```protobuf
message SubscribeResult {
    // Existing fields
    uint64 offset = 4;
    string epoch = 5;
    repeated Publication publications = 6;  // History for recovery
    bool recovered = 7;

    // NEW: Indicates keyed mode
    bool keyed = 10;

    // When keyed=true, publications field contains initial snapshot
    // (instead of history)
}
```

**For regular subscriptions:**
```json
{
  "keyed": false,
  "publications": [...],  // History (if recovery)
  "offset": 42,
  "epoch": "abc"
}
```

**For keyed subscriptions:**
```json
{
  "keyed": true,
  "publications": [       // Initial snapshot
    {
      "key": "user:123",
      "data": "...",
      "offset": 42,
      "removed": false,
      "tags": {"score": "100"}
    },
    {
      "key": "user:456",
      "data": "...",
      "offset": 41,
      "removed": false,
      "tags": {"score": "95"}
    }
  ],
  "offset": 42,
  "epoch": "abc"
}
```

### 3. Push Messages (No Changes!)

```protobuf
message Push {
    string channel = 2;
    Publication pub = 4;  // ← Same Publication type!
    // ... other push types
}
```

**Regular publication:**
```json
{
  "channel": "chat:lobby",
  "pub": {
    "data": "hello",
    "offset": 43
  }
}
```

**Keyed update (add/update):**
```json
{
  "channel": "users",
  "pub": {
    "key": "user:123",        // ← Key present
    "data": "{...}",
    "offset": 43,
    "removed": false,          // ← Not removed
    "tags": {"score": "100"}
  }
}
```

**Keyed remove:**
```json
{
  "channel": "users",
  "pub": {
    "key": "user:123",        // ← Key present
    "offset": 44,
    "removed": true,           // ← Removed flag
    "tags": {"score": "100"}   // Optional: last score
  }
}
```

### 4. Client Commands (Reuse Publish)

**No new commands needed!** Use existing publish with `key` field:

```protobuf
message PublishRequest {
    string channel = 1;
    bytes data = 2;

    // NEW: For keyed publishes
    string key = 10;              // If set, this is a keyed publish
    bool removed = 11;            // If true, this is a remove
    map<string, string> tags = 12; // For score, etc.
}

message PublishResult {
    // Existing
    uint64 offset = 1;
    string epoch = 2;
}
```

**Regular publish (unchanged):**
```json
{
  "channel": "chat:lobby",
  "data": "hello"
}
```

**Keyed publish (add/update):**
```json
{
  "channel": "users",
  "key": "user:123",
  "data": "{...}",
  "removed": false,
  "tags": {"score": "100"}
}
```

**Keyed remove:**
```json
{
  "channel": "users",
  "key": "user:123",
  "removed": true
}
```

---

## Client SDK Design

### JavaScript Example

```javascript
// Regular subscription (unchanged)
const chat = centrifuge.newSubscription('chat:lobby');
chat.on('publication', (ctx) => {
    console.log('Message:', ctx.data);
});
chat.subscribe();

// Keyed subscription
const users = centrifuge.newSubscription('users');
// Subscribe - server returns keyed=true + snapshot in publications array
users.subscribe();

// SDK detects keyed mode from subscribe result
// and fires different events based on Publication.removed flag

users.on('subscribing', () => {});

users.on('subscribed', (ctx) => {
    if (ctx.keyed) {
        // This is a keyed subscription
        // ctx.publications contains initial snapshot
        console.log('Snapshot:', ctx.publications);
    }
});

users.on('publication', (ctx) => {
    // SDK fires this for all Publications

    if (ctx.data.key) {
        // This is a keyed publication
        if (ctx.data.removed) {
            console.log('Removed:', ctx.data.key);
        } else {
            console.log('Updated:', ctx.data.key, ctx.data.data);
        }
    } else {
        // Regular publication
        console.log('Message:', ctx.data.data);
    }
});

// Or use convenience helpers
users.on('publication', (ctx) => {
    if (ctx.isKeyed()) {
        if (ctx.isRemoved()) {
            console.log('Removed:', ctx.getKey());
        } else {
            console.log('Updated:', ctx.getKey(), ctx.getData());
        }
    }
});
```

### Alternative: Separate Event Emitters

SDK can still provide separate events for better DX:

```javascript
const users = centrifuge.newSubscription('users');

// SDK internally checks Publication.key and Publication.removed
// and emits appropriate events

users.on('snapshot', (ctx) => {
    // Fired on subscribe if ctx.keyed=true
    // ctx.entries = array of Publications with key set
    ctx.entries.forEach(pub => {
        console.log(pub.key, pub.data);
    });
});

users.on('update', (ctx) => {
    // Fired when Publication has key && !removed
    console.log('Updated:', ctx.key, ctx.data);
});

users.on('remove', (ctx) => {
    // Fired when Publication has key && removed
    console.log('Removed:', ctx.key);
});

users.on('publication', (ctx) => {
    // Still available - fires for ALL publications
    // User can check ctx.key manually if needed
});

users.subscribe();
```

### Client Methods

```javascript
// Publish to keyed channel
centrifuge.publish('users', {
    key: 'user:123',
    data: JSON.stringify({name: 'Alice'}),
    tags: {score: '100'}
}).then(result => {
    console.log('Offset:', result.offset);
});

// Remove from keyed channel
centrifuge.publish('users', {
    key: 'user:123',
    removed: true
}).then(result => {
    console.log('Removed at offset:', result.offset);
});

// Or convenience methods
centrifuge.keyedPublish('users', 'user:123', {name: 'Alice'}, {score: 100});
centrifuge.keyedRemove('users', 'user:123');
```

---

## Keyed Presence (Using Publications)

### Server-Side Config

```go
node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
    return ConnectReply{
        Subscriptions: map[string]SubscribeOptions{
            "presence:lobby": {
                Keyed: true,
                KeyedPresence: true,  // SDK auto-publishes presence
            },
        },
    }, nil
})
```

### Client-Side (Auto Presence)

```javascript
const presence = centrifuge.newSubscription('presence:lobby');

presence.subscribe();

// On subscribed event, if keyed_presence=true:
presence.on('subscribed', (ctx) => {
    if (ctx.keyedPresence) {
        // SDK automatically publishes client presence
        centrifuge.publish('presence:lobby', {
            key: centrifuge.clientID,  // Use client ID as key
            data: JSON.stringify({
                user: 'alice',
                connInfo: {...}
            })
        });

        // SDK starts periodic keepalive
        // SDK removes presence on unsubscribe
    }

    // ctx.publications contains current presence snapshot
    console.log('Online users:', ctx.publications.map(p => p.key));
});

// Handle updates
presence.on('publication', (ctx) => {
    if (ctx.data.key) {
        if (ctx.data.removed) {
            console.log('User left:', ctx.data.key);
        } else {
            console.log('User joined:', ctx.data.key);
        }
    }
});

// Or convenience events
presence.on('update', (ctx) => {
    console.log('User joined:', ctx.key, JSON.parse(ctx.data));
});

presence.on('remove', (ctx) => {
    console.log('User left:', ctx.key);
});
```

---

## Score Handling

For ordered snapshots, use the `tags` field:

```javascript
// Server-side: Publish with score
node.KeyedPublish(ctx, "leaderboard", "user:123", data,
    centrifuge.WithOrdered(true),
    centrifuge.WithScore(9500),
)

// Wire format (Publication):
{
  "key": "user:123",
  "data": "{...}",
  "tags": {"score": "9500"},  // Score in tags
  "offset": 42
}

// Client receives Publication
sub.on('update', (ctx) => {
    const score = parseInt(ctx.data.tags.score || '0');
    console.log(ctx.key, 'has score', score);
});

// Snapshot is already sorted by score (server-side)
sub.on('snapshot', (ctx) => {
    // ctx.entries are sorted descending by score
    ctx.entries.forEach((pub, rank) => {
        console.log(`#${rank+1}: ${pub.key} - ${pub.tags.score}`);
    });
});
```

### Alternative: Add Score Field to Publication

If we want cleaner API, add one field:

```go
type Publication struct {
    // ... existing fields
    Key     string
    Removed bool
    Score   int64  // ← Add this for ordered snapshots
}
```

Then:
```javascript
sub.on('update', (ctx) => {
    console.log(ctx.key, 'has score', ctx.data.score);
});
```

---

## Protocol Summary

### What Changes

**SubscribeResult** (add one field):
```protobuf
message SubscribeResult {
    // ... existing fields
    bool keyed = 10;  // ← NEW: indicates keyed mode
    bool keyed_presence = 11;  // ← NEW: SDK should auto-publish presence
}
```

**PublishRequest** (add optional fields):
```protobuf
message PublishRequest {
    // ... existing fields
    string key = 10;  // ← NEW: for keyed publishes
    bool removed = 11;  // ← NEW: for removes
}
```

**Publication** (optionally add):
```protobuf
message Publication {
    // ... existing fields
    int64 score = 20;  // ← OPTIONAL: cleaner than tags
}
```

### What Stays the Same

- ✅ `Publication` type used for everything
- ✅ `Push` message unchanged
- ✅ Subscribe/unsubscribe flow unchanged
- ✅ Recovery/positioning works the same
- ✅ Client protocol version compatible

---

## Benefits of Reusing Publication

1. **Minimal protocol changes** - just add flags, reuse existing types
2. **Unified handling** - clients already know how to handle Publications
3. **Recovery works automatically** - Publications have offset/epoch
4. **Presence already works** - Publications have key/removed for presence
5. **Simpler SDK** - less code to handle different message types
6. **Backward compatible** - old clients ignore new fields

---

## Complete Example

### Server

```go
// Create node with KeyedEngine
node, _ := centrifuge.New(centrifuge.Config{})
keyedEngine, _ := centrifuge.NewRedisKeyedEngine(node, ...)
node.SetKeyedEngine(keyedEngine)
node.Run()

// Configure keyed channel
node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
    return ConnectReply{
        Subscriptions: map[string]SubscribeOptions{
            "users": {
                Keyed: true,
                KeyedOrdered: true,
            },
            "presence:*": {
                Keyed: true,
                KeyedPresence: true,
            },
        },
    }, nil
})

// Publish to keyed channel
node.KeyedPublish(ctx, "users", "user:123", data,
    centrifuge.WithOrdered(true),
    centrifuge.WithScore(100),
)
```

### Client (JavaScript)

```javascript
const centrifuge = new Centrifuge('ws://localhost:8000/connection/websocket');

// Subscribe to keyed channel
const users = centrifuge.newSubscription('users');

users.on('subscribed', (ctx) => {
    if (ctx.keyed) {
        console.log('Initial snapshot:', ctx.publications);
    }
});

users.on('publication', (ctx) => {
    if (ctx.data.key) {
        // Keyed publication
        if (ctx.data.removed) {
            console.log('Removed:', ctx.data.key);
        } else {
            console.log('Updated:', ctx.data.key);
            console.log('Score:', ctx.data.tags.score);
        }
    }
});

// Or use convenience events (SDK provides)
users.on('update', (ctx) => {
    console.log('Updated:', ctx.key, ctx.data, ctx.score);
});

users.on('remove', (ctx) => {
    console.log('Removed:', ctx.key);
});

users.subscribe();

// Subscribe to presence
const presence = centrifuge.newSubscription('presence:lobby');

presence.on('subscribed', (ctx) => {
    // SDK auto-publishes our presence if keyedPresence=true
    console.log('Online users:', ctx.publications.map(p => p.key));
});

presence.on('update', (ctx) => {
    console.log('User joined:', ctx.key);
});

presence.on('remove', (ctx) => {
    console.log('User left:', ctx.key);
});

presence.subscribe();

centrifuge.connect();
```

---

## Summary

**By reusing `Publication` type:**

1. **Minimal protocol changes** - just add `keyed` flag to SubscribeResult
2. **No new message types** - Publication handles everything
3. **Simpler implementation** - same code paths for regular and keyed
4. **Cleaner** - Publications already have `key` and `removed` fields
5. **Score via tags** - or add one `score` field to Publication
6. **Presence works** - Publications perfect for presence (key=clientID, removed=left)

**The protocol becomes:**
- Subscribe response includes `keyed: true` → snapshot in `publications` array
- Push messages contain Publications with `key` set and `removed` flag
- Client SDK interprets based on subscription mode
- Minimal changes, maximum reuse!
