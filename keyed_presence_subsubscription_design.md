# KeyedEngine: Presence as Sub-Subscription

## Core Principle

**Presence is a property of the main subscription.** It's accessed via `subscription.presence` and its lifecycle is completely tied to the parent subscription.

**Benefits:**
- ✅ Single permission check (main subscription grants access to presence)
- ✅ Clear lifecycle (presence lives/dies with parent subscription)
- ✅ No ordering issues (can't subscribe to presence before main channel)
- ✅ Intuitive API (presence is a feature of the subscription)

---

## Client API

### Regular Subscription (No Presence)

```javascript
const chat = centrifuge.newSubscription('chat:lobby');

chat.on('subscribed', (ctx) => {
    console.log('Subscribed to chat');
});

chat.on('publication', (ctx) => {
    console.log('Message:', ctx.data);
});

chat.subscribe();
```

### Subscription with Presence

```javascript
const chat = centrifuge.newSubscription('chat:lobby');

// Main subscription events
chat.on('subscribed', (ctx) => {
    console.log('Subscribed to chat');
    console.log('Recovered:', ctx.recovered);
});

chat.on('publication', (ctx) => {
    console.log('Message:', ctx.data);
});

// Presence sub-subscription (accessed via .presence property)
chat.presence.on('subscribed', (ctx) => {
    // ctx.publications - full presence snapshot
    console.log('Online users:', ctx.publications.length);

    ctx.publications.forEach(pub => {
        const info = JSON.parse(pub.data);
        console.log('User:', info.userID, 'Client:', pub.key);
    });
});

chat.presence.on('publication', (ctx) => {
    // Presence updates
    if (ctx.data.removed) {
        console.log('User left:', ctx.data.key);
    } else {
        const info = JSON.parse(ctx.data.data);
        console.log('User joined:', info.userID);
    }
});

// Subscribe to main channel first (required!)
chat.subscribe();

// Then subscribe to presence (optional)
chat.presence.subscribe();
```

### Keyed Subscription with Presence

```javascript
const users = centrifuge.newSubscription('users', {keyed: true});

// Main subscription (keyed data)
users.on('subscribed', (ctx) => {
    console.log('User snapshot:', ctx.publications);
    console.log('Recovered:', ctx.recovered);
});

users.on('publication', (ctx) => {
    if (ctx.data.removed) {
        console.log('User removed:', ctx.data.key);
    } else {
        console.log('User updated:', ctx.data.key);
    }
});

// Presence sub-subscription (also keyed!)
users.presence.on('subscribed', (ctx) => {
    console.log('Online connections:', ctx.publications.length);
});

users.presence.on('publication', (ctx) => {
    if (ctx.data.removed) {
        console.log('Connection left:', ctx.data.key);
    } else {
        console.log('Connection joined:', ctx.data.key);
    }
});

users.subscribe();
users.presence.subscribe();
```

---

## Protocol Design

### Subscribe Request (Extended)

```protobuf
message SubscribeRequest {
    string channel = 1;

    // Existing fields
    uint64 recover_since_offset = 2;
    string recover_since_epoch = 3;

    // NEW: Keyed support
    bool keyed = 10;
    Phase phase = 11;       // SNAPSHOT | STREAM
    int32 limit = 12;       // Pagination limit
    string cursor = 13;     // Pagination cursor
    uint64 position_offset = 14;
    string position_epoch = 15;

    // NEW: Presence sub-subscription
    bool presence = 20;     // ← Indicates this is a presence sub-subscription
}

enum Phase {
    SNAPSHOT = 0;
    STREAM = 1;
}
```

### Subscribe Result (Extended)

```protobuf
message SubscribeResult {
    // Existing fields
    uint64 offset = 4;
    string epoch = 5;
    repeated Publication publications = 6;
    bool recovered = 7;

    // NEW: Keyed support
    bool keyed = 10;
    string cursor = 11;
    Phase phase = 12;

    // NEW: Presence capability
    bool presence_available = 20;  // ← Server indicates presence is available for this channel
}
```

**Key insight:** The `presence` flag in SubscribeRequest indicates this is a **presence sub-subscription** of an already established main subscription.

---

## Protocol Flow

### Flow 1: Regular Channel with Presence (Single Page Snapshot)

```
Client                                Server
  |                                     |
  | SubscribeRequest                   |
  |   channel="chat:lobby"             |
  |   keyed=false                      |
  |   presence=false                   |
  | ----------------------------------> |
  |                                     | (Check permissions)
  |                                     | (Main subscription OK)
  |                                     |
  |          SubscribeResult            |
  |   keyed=false                       |
  |   presence_available=true ←-----   | (Server indicates presence is available)
  | <---------------------------------- |
  |                                     |
  | [Main subscription established]    |
  |                                     |
  | SubscribeRequest                   |
  |   channel="chat:lobby"             |
  |   keyed=true ←-------------------  | (Presence is always keyed!)
  |   presence=true ←-----------------  | (This is presence sub-subscription)
  |   phase=SNAPSHOT                   |
  |   limit=100                        |
  |   cursor=""                        |
  | ----------------------------------> |
  |                                     | (Check main subscription exists)
  |                                     | (Fetch presence snapshot from chat:lobby:presence)
  |                                     |
  |          SubscribeResult            |
  |   keyed=true                        |
  |   publications=[50 entries] ←---   | (Presence snapshot - all users fit in one page)
  |   cursor="" ←---------------------- | (Empty cursor = last page)
  |   offset=42                        |
  |   epoch="abc"                      |
  |   phase=SNAPSHOT                   |
  | <---------------------------------- |
  |                                     |
  | (Snapshot complete, switch to stream) |
  |                                     |
  | SubscribeRequest                   |
  |   channel="chat:lobby"             |
  |   keyed=true                       |
  |   presence=true                    |
  |   phase=STREAM                     |
  |   position_offset=42               |
  |   position_epoch="abc"             |
  | ----------------------------------> |
  |                                     | (Start auto-maintaining this client's presence)
  |                                     | (Publish initial presence for this client)
  |                                     |
  |          SubscribeResult            |
  |   phase=STREAMING                  |
  | <---------------------------------- |
  |                                     |
  | [Presence sub-subscription ready]  |
  |                                     |
  |           Push (Publication)        |
  |   channel="chat:lobby:presence" ←  | (Presence publication!)
  |   pub.key="client:xyz"             |
  |   pub.removed=false                |
  | <---------------------------------- | (New user joined)
  |                                     |
```

### Flow 1b: Regular Channel with Presence (Multi-Page Snapshot)

If there are many online users, presence snapshot is paginated:

```
Client                                Server
  |                                     |
  | [Main subscription already established] |
  |                                     |
  | SubscribeRequest (Page 1)          |
  |   channel="chat:lobby"             |
  |   keyed=true                       |
  |   presence=true                    |
  |   phase=SNAPSHOT                   |
  |   limit=100                        |
  |   cursor=""                        |
  | ----------------------------------> |
  |                                     |
  |          SubscribeResult            |
  |   publications=[100 entries]       |
  |   cursor="page2_token"             |
  |   offset=42                        |
  |   epoch="abc"                      |
  | <---------------------------------- |
  |                                     |
  | SubscribeRequest (Page 2)          |
  |   channel="chat:lobby"             |
  |   keyed=true                       |
  |   presence=true                    |
  |   phase=SNAPSHOT                   |
  |   limit=100                        |
  |   cursor="page2_token"             |
  | ----------------------------------> |
  |                                     |
  |          SubscribeResult            |
  |   publications=[100 entries]       |
  |   cursor="page3_token"             |
  | <---------------------------------- |
  |                                     |
  | (Continue until cursor empty...)   |
  |                                     |
  | SubscribeRequest (Last Page)       |
  |   cursor="pageN_token"             |
  | ----------------------------------> |
  |                                     |
  |          SubscribeResult            |
  |   publications=[30 entries]        |
  |   cursor="" ←--------------------- | (Last page)
  |   offset=42                        |
  |   epoch="abc"                      |
  | <---------------------------------- |
  |                                     |
  | (All 230 users loaded)             |
  |                                     |
  | SubscribeRequest                   |
  |   phase=STREAM                     |
  |   position_offset=42               |
  |   position_epoch="abc"             |
  | ----------------------------------> |
  |                                     |
  |          SubscribeResult            |
  |   phase=STREAMING                  |
  | <---------------------------------- |
  |                                     |
  | [Streaming real-time updates]      |
  |                                     |
```

### Flow 2: Keyed Channel with Presence

```
Client                                Server
  |                                     |
  | SubscribeRequest                   |
  |   channel="users"                  |
  |   keyed=true ←--------------------  | (User requested keyed)
  |   presence=false                   |
  |   phase=SNAPSHOT                   |
  | ----------------------------------> |
  |                                     | (Check permissions)
  |                                     | (Fetch keyed snapshot)
  |                                     |
  |          SubscribeResult            |
  |   keyed=true                        |
  |   publications=[...] ←-----------  | (User data snapshot)
  |   presence_available=true          |
  | <---------------------------------- |
  |                                     |
  | (Client iterates snapshot pages)   |
  |                                     |
  | SubscribeRequest                   |
  |   channel="users"                  |
  |   keyed=true                       |
  |   phase=STREAM                     |
  | ----------------------------------> |
  |                                     |
  | [Main keyed subscription ready]    |
  |                                     |
  | SubscribeRequest                   |
  |   channel="users"                  |
  |   keyed=true                       |
  |   presence=true ←-----------------  | (Presence sub-subscription)
  |   phase=SNAPSHOT                   |
  | ----------------------------------> |
  |                                     | (Check main subscription exists)
  |                                     | (Fetch presence snapshot)
  |                                     |
  |          SubscribeResult            |
  |   keyed=true                        |
  |   publications=[...] ←-----------  | (Presence snapshot)
  | <---------------------------------- |
  |                                     |
  | (Client iterates pages)            |
  |                                     |
  | SubscribeRequest                   |
  |   channel="users"                  |
  |   keyed=true                       |
  |   presence=true                    |
  |   phase=STREAM                     |
  | ----------------------------------> |
  |                                     |
  | [Presence sub-subscription ready]  |
  |                                     |
```

---

## Server-Side Implementation

### Channel Representation

**Channel naming convention:**
- Main channel: `"chat:lobby"` → stored as `"chat:lobby"`
- Presence for main channel: `"chat:lobby"` + `:presence` suffix → stored as `"chat:lobby:presence"`

**SDK implicitly adds `:presence` suffix** when subscribing to presence sub-subscription.

**Benefits of `:presence` suffix approach:**
- ✅ Publications naturally have `channel="chat:lobby:presence"` - SDK easily routes by checking suffix
- ✅ Clean channel naming (not a hidden prefix)
- ✅ Permissions can be configured with patterns like `*:presence` if needed
- ✅ Clear separation between main data and presence data

**The presence flag in SubscribeRequest tells the server:**
1. This is a presence sub-subscription (not main data)
2. Check that main subscription exists for the base channel (`"chat:lobby"`)
3. Use `{channel}:presence` for storage (`"chat:lobby:presence"`)
4. Auto-maintain this client's presence

### Subscription Tracking

```go
type Client struct {
    // ... existing fields

    subscriptions map[string]*Subscription  // Main subscriptions
    presenceSubs  map[string]*Subscription  // Presence sub-subscriptions
}

type Subscription struct {
    Channel  string
    Keyed    bool
    Presence bool  // ← If true, actual channel is {Channel}:presence

    // ... other fields
}

// Helper to get actual channel name
func (s *Subscription) actualChannel() string {
    if s.Presence {
        return s.Channel + ":presence"
    }
    return s.Channel
}
```

### Subscribe Handler

```go
func (c *Client) handleSubscribe(req *SubscribeRequest) (*SubscribeResult, error) {
    if req.Presence {
        // This is a presence sub-subscription
        return c.handlePresenceSubscribe(req)
    }

    // This is a main subscription
    return c.handleMainSubscribe(req)
}

func (c *Client) handlePresenceSubscribe(req *SubscribeRequest) (*SubscribeResult, error) {
    // 1. Verify main subscription exists
    mainSub, exists := c.subscriptions[req.Channel]
    if !exists {
        return nil, ErrorNotSubscribed  // Must subscribe to main channel first!
    }

    // 2. Check if presence already active for this channel
    if _, exists := c.presenceSubs[req.Channel]; exists {
        return nil, ErrorAlreadySubscribed
    }

    // 3. Construct presence channel name with :presence suffix
    presenceChannel := req.Channel + ":presence"

    // 4. Presence is always keyed!
    if !req.Keyed {
        return nil, ErrorPresenceMustBeKeyed
    }

    // 5. Handle two-phase subscribe for presence
    if req.Phase == SubscribeRequest_SNAPSHOT {
        // Fetch presence snapshot
        result, err := c.fetchPresenceSnapshot(presenceChannel, req.Limit, req.Cursor)
        if err != nil {
            return nil, err
        }

        return result, nil
    }

    if req.Phase == SubscribeRequest_STREAM {
        // Switch to streaming
        result, err := c.switchPresenceToStreaming(presenceChannel, req.PositionOffset, req.PositionEpoch)
        if err != nil {
            return nil, err
        }

        // 6. Start auto-maintaining this client's presence
        c.startPresenceMaintenance(presenceChannel)

        // 7. Track presence sub-subscription
        c.presenceSubs[req.Channel] = &Subscription{
            Channel:  req.Channel,
            Keyed:    true,
            Presence: true,
        }

        return result, nil
    }

    return nil, ErrorInvalidPhase
}

func (c *Client) handleMainSubscribe(req *SubscribeRequest) (*SubscribeResult, error) {
    // Check permissions
    if !c.isAllowed(req.Channel) {
        return nil, ErrorPermissionDenied
    }

    // Check if channel supports presence
    channelOpts := c.node.channelOpts(req.Channel)
    presenceAvailable := channelOpts.EnablePresence

    if req.Keyed {
        // Keyed subscription (two-phase)
        return c.handleKeyedSubscribe(req, presenceAvailable)
    }

    // Regular subscription
    return c.handleRegularSubscribe(req, presenceAvailable)
}

func (c *Client) startPresenceMaintenance(presenceChannel string) {
    // Publish initial presence
    presenceInfo := &ClientInfo{
        ClientID: c.ID(),
        UserID:   c.UserID(),
        ConnInfo: c.ConnInfo(),
        ChanInfo: c.ChanInfo(),
    }

    data, _ := json.Marshal(presenceInfo)

    // Initial publish
    c.node.KeyedPublish(c.ctx, presenceChannel, c.ID(), data,
        centrifuge.WithExpire(60*time.Second),
    )

    // Start keepalive goroutine
    go func() {
        ticker := time.NewTicker(25 * time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ticker.C:
                // Refresh presence
                c.node.KeyedPublish(c.ctx, presenceChannel, c.ID(), data,
                    centrifuge.WithExpire(60*time.Second),
                )

            case <-c.ctx.Done():
                // Remove presence on disconnect
                c.node.KeyedRemove(c.ctx, presenceChannel, c.ID())
                return
            }
        }
    }()
}
```

### Unsubscribe Handler

```go
func (c *Client) handleUnsubscribe(req *UnsubscribeRequest) error {
    if req.Presence {
        // Unsubscribe from presence sub-subscription
        return c.handlePresenceUnsubscribe(req)
    }

    // Unsubscribe from main subscription
    return c.handleMainUnsubscribe(req)
}

func (c *Client) handlePresenceUnsubscribe(req *UnsubscribeRequest) error {
    presenceSub, exists := c.presenceSubs[req.Channel]
    if !exists {
        return ErrorNotSubscribed
    }

    // Stop auto-maintaining presence
    c.stopPresenceMaintenance(presenceSub.actualChannel())

    // Remove from tracking
    delete(c.presenceSubs, req.Channel)

    return nil
}

func (c *Client) handleMainUnsubscribe(req *UnsubscribeRequest) error {
    // 1. Check if presence sub-subscription exists
    if presenceSub, exists := c.presenceSubs[req.Channel]; exists {
        // Auto-unsubscribe from presence first!
        c.handlePresenceUnsubscribe(&UnsubscribeRequest{
            Channel:  req.Channel,
            Presence: true,
        })
    }

    // 2. Unsubscribe from main subscription
    delete(c.subscriptions, req.Channel)

    return nil
}
```

---

## SDK Implementation (JavaScript)

### Subscription Class

```javascript
class Subscription {
    constructor(centrifuge, channel, opts) {
        this._centrifuge = centrifuge;
        this._channel = channel;
        this._keyed = opts.keyed || false;
        this._state = 'unsubscribed';

        // Create presence sub-subscription property
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

    async unsubscribe() {
        // Auto-unsubscribe presence if active
        if (this._presence.isSubscribed()) {
            await this._presence.unsubscribe();
        }

        // Unsubscribe main
        await this._centrifuge._sendUnsubscribe({
            channel: this._channel,
            presence: false
        });

        this._setState('unsubscribed');
    }

    async _regularSubscribe() {
        const result = await this._centrifuge._sendSubscribe({
            channel: this._channel,
            keyed: false,
            presence: false
        });

        this._setState('subscribed');
        this._emit('subscribed', {
            channel: this._channel,
            recovered: result.recovered,
            presenceAvailable: result.presenceAvailable  // ← Server tells us
        });
    }

    async _keyedSubscribe() {
        // Phase 1: Fetch complete snapshot
        const snapshot = await this._fetchCompleteSnapshot();

        // Phase 2: Switch to streaming
        await this._switchToStreaming(snapshot.offset, snapshot.epoch);

        this._setState('subscribed');
        this._emit('subscribed', {
            channel: this._channel,
            recovered: false,
            publications: snapshot.publications,
            offset: snapshot.offset,
            epoch: snapshot.epoch,
            presenceAvailable: snapshot.presenceAvailable
        });
    }

    async _fetchCompleteSnapshot() {
        let cursor = '';
        let allPublications = [];
        let finalOffset = 0;
        let finalEpoch = '';
        let presenceAvailable = false;

        while (true) {
            const result = await this._centrifuge._sendSubscribe({
                channel: this._channel,
                keyed: true,
                presence: false,
                phase: 'snapshot',
                limit: 100,
                cursor: cursor
            });

            allPublications.push(...result.publications);
            finalOffset = result.offset;
            finalEpoch = result.epoch;
            presenceAvailable = result.presenceAvailable;

            if (result.cursor === '') {
                break;
            }

            cursor = result.cursor;
        }

        return {
            publications: allPublications,
            offset: finalOffset,
            epoch: finalEpoch,
            presenceAvailable: presenceAvailable
        };
    }

    async _switchToStreaming(offset, epoch) {
        return await this._centrifuge._sendSubscribe({
            channel: this._channel,
            keyed: this._keyed,
            presence: false,
            phase: 'stream',
            position_offset: offset,
            position_epoch: epoch
        });
    }
}
```

### PresenceSubscription Class

```javascript
class PresenceSubscription {
    constructor(parentSubscription) {
        this._parent = parentSubscription;
        this._centrifuge = parentSubscription._centrifuge;
        this._channel = parentSubscription._channel;
        this._state = 'unsubscribed';
        this._handlers = {};
    }

    on(event, handler) {
        if (!this._handlers[event]) {
            this._handlers[event] = [];
        }
        this._handlers[event].push(handler);
    }

    isSubscribed() {
        return this._state === 'subscribed';
    }

    async subscribe() {
        // 1. Check parent is subscribed
        if (this._parent._state !== 'subscribed') {
            throw new Error('Must subscribe to main channel first');
        }

        // 2. Fetch presence snapshot (two-phase)
        const snapshot = await this._fetchPresenceSnapshot();

        // 3. Switch to streaming
        await this._switchPresenceToStreaming(snapshot.offset, snapshot.epoch);

        this._setState('subscribed');
        this._emit('subscribed', {
            channel: this._channel,
            recovered: false,
            publications: snapshot.publications,
            offset: snapshot.offset,
            epoch: snapshot.epoch
        });
    }

    async _fetchPresenceSnapshot() {
        let cursor = '';
        let allPublications = [];
        let finalOffset = 0;
        let finalEpoch = '';

        while (true) {
            const result = await this._centrifuge._sendSubscribe({
                channel: this._channel,
                keyed: true,        // ← Presence is always keyed!
                presence: true,     // ← This is presence sub-subscription
                phase: 'snapshot',
                limit: 100,
                cursor: cursor
            });

            allPublications.push(...result.publications);
            finalOffset = result.offset;
            finalEpoch = result.epoch;

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

    async _switchPresenceToStreaming(offset, epoch) {
        return await this._centrifuge._sendSubscribe({
            channel: this._channel,
            keyed: true,
            presence: true,
            phase: 'stream',
            position_offset: offset,
            position_epoch: epoch
        });
    }

    async unsubscribe() {
        await this._centrifuge._sendUnsubscribe({
            channel: this._channel,
            presence: true  // ← Unsubscribe from presence sub-subscription
        });

        this._setState('unsubscribed');
    }

    _handlePublication(pub) {
        this._emit('publication', {
            channel: this._channel,
            data: pub
        });
    }

    _emit(event, ctx) {
        if (this._handlers[event]) {
            this._handlers[event].forEach(h => h(ctx));
        }
    }

    _setState(state) {
        this._state = state;
    }
}
```

### Centrifuge Class (Message Routing)

```javascript
class Centrifuge {
    // ... existing code

    _handlePush(push) {
        if (push.pub) {
            // Route to correct subscription
            const sub = this._subscriptions[push.channel];
            if (sub) {
                // Check if this is a presence publication
                if (sub._presence.isSubscribed()) {
                    // Route to presence sub-subscription
                    sub._presence._handlePublication(push.pub);
                } else {
                    // Route to main subscription
                    sub._handlePublication(push.pub);
                }
            }
        }
    }
}
```

**Problem:** How does the SDK know if a publication is for main or presence?

**Solution:** The server sends publications with channel name indicating presence:
- Main channel publications: `channel="chat:lobby"`
- Presence publications: `channel="chat:lobby:presence"`

SDK routes based on `:presence` suffix.

### Updated Message Routing

```javascript
class Centrifuge {
    _handlePush(push) {
        if (push.pub) {
            // Check if this is a presence publication
            if (push.channel.endsWith(':presence')) {
                // Extract original channel name (remove ":presence" suffix)
                const originalChannel = push.channel.slice(0, -9);  // Remove ":presence"

                const sub = this._subscriptions[originalChannel];
                if (sub && sub._presence.isSubscribed()) {
                    sub._presence._handlePublication(push.pub);
                }
            } else {
                // Regular publication
                const sub = this._subscriptions[push.channel];
                if (sub) {
                    sub._handlePublication(push.pub);
                }
            }
        }
    }
}
```

---

## Complete Example

### Server Setup

```go
node, _ := centrifuge.New(centrifuge.Config{})

// Set KeyedEngine
keyedEngine, _ := centrifuge.NewRedisKeyedEngine(node, ...)
node.SetKeyedEngine(keyedEngine)

// Configure channel
node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
    return ConnectReply{
        Subscriptions: map[string]SubscribeOptions{
            "chat:lobby": {
                EnablePresence: true,  // ← Enables presence for this channel
            },
        },
    }, nil
})

node.Run()
```

### Client Usage

```javascript
const centrifuge = new Centrifuge('ws://localhost:8000/connection/websocket');

// Subscribe to chat
const chat = centrifuge.newSubscription('chat:lobby');

chat.on('subscribed', (ctx) => {
    console.log('Subscribed to chat');

    if (ctx.presenceAvailable) {
        console.log('Presence is available for this channel!');
    }
});

chat.on('publication', (ctx) => {
    console.log('Chat message:', ctx.data);
});

// Subscribe to presence (optional)
chat.presence.on('subscribed', (ctx) => {
    console.log('Presence snapshot:');
    ctx.publications.forEach(pub => {
        const info = JSON.parse(pub.data);
        console.log(' -', info.userID, '(client:', pub.key + ')');
    });
});

chat.presence.on('publication', (ctx) => {
    if (ctx.data.removed) {
        console.log('User left:', ctx.data.key);
    } else {
        const info = JSON.parse(ctx.data.data);
        console.log('User joined:', info.userID);
    }
});

// Connect and subscribe
centrifuge.connect();
chat.subscribe();

// Subscribe to presence after main subscription
setTimeout(() => {
    chat.presence.subscribe();
}, 1000);
```

---

## Lifecycle Management

### Scenario 1: Subscribe to Main, Then Presence

```
1. chat.subscribe() → main subscription established
2. chat.presence.subscribe() → presence sub-subscription established
3. Server starts auto-maintaining this client's presence
```

### Scenario 2: Unsubscribe from Presence

```
1. chat.presence.unsubscribe() → presence sub-subscription closed
2. Server stops auto-maintaining this client's presence
3. Server publishes remove for this client
4. Main subscription still active
```

### Scenario 3: Unsubscribe from Main

```
1. chat.unsubscribe() → main subscription closing
2. SDK auto-unsubscribes from presence
3. Server stops auto-maintaining presence
4. Server publishes remove for this client
5. Both subscriptions closed
```

### Scenario 4: Reconnect

```
1. Connection lost
2. On reconnect, SDK re-subscribes to main channel
3. If presence was active, SDK also re-subscribes to presence
4. Server resumes auto-maintaining presence
```

SDK tracks which sub-subscriptions were active and restores them on reconnect.

```javascript
class Subscription {
    async _resubscribe() {
        // Resubscribe to main
        await this.subscribe();

        // If presence was active, resubscribe
        if (this._presence._wasSubscribed) {
            await this._presence.subscribe();
        }
    }
}
```

---

## Permission Model

**Single permission check:** When client subscribes to main channel, server checks permissions once.

**Presence access:** If main subscription succeeds, client can subscribe to presence (no additional permission check).

**Server-side:**

```go
func (c *Client) handleSubscribe(req *SubscribeRequest) (*SubscribeResult, error) {
    if req.Presence {
        // Presence sub-subscription
        // Only check that main subscription exists (no permission check!)
        mainSub, exists := c.subscriptions[req.Channel]
        if !exists {
            return nil, ErrorNotSubscribed
        }

        // OK, proceed with presence subscribe
        return c.handlePresenceSubscribe(req)
    }

    // Main subscription
    // THIS is where permission check happens
    if !c.isAllowed(req.Channel) {
        return nil, ErrorPermissionDenied
    }

    return c.handleMainSubscribe(req)
}
```

---

## Summary

**Presence as sub-subscription solves all problems:**

1. ✅ **Permission:** Single check for main subscription (presence inherits access)
2. ✅ **Lifecycle:** Presence tied to main subscription (can't exist independently)
3. ✅ **Ordering:** Must subscribe to main first (SDK enforces)
4. ✅ **API clarity:** `subscription.presence` makes relationship obvious
5. ✅ **Protocol reuse:** Same two-phase subscribe for presence (keyed snapshot + stream)
6. ✅ **Auto-maintenance:** Server automatically publishes/removes client presence
7. ✅ **Unsubscribe:** Unsubscribing from main auto-unsubscribes presence

**Protocol additions:**

```protobuf
message SubscribeRequest {
    // ... existing fields
    bool presence = 20;  // ← NEW: indicates presence sub-subscription
}

message SubscribeResult {
    // ... existing fields
    bool presence_available = 20;  // ← NEW: server indicates presence capability
}
```

**Client API:**

```javascript
// Main subscription
const chat = centrifuge.newSubscription('chat:lobby');
chat.subscribe();

// Presence sub-subscription (accessed via .presence)
chat.presence.subscribe();
chat.presence.on('subscribed', (ctx) => { ... });
chat.presence.on('publication', (ctx) => { ... });
```

**Server internally:**
- Main channel: `"chat:lobby"`
- Presence channel: `"chat:lobby:presence"` (`:presence` suffix added by SDK)
- Auto-maintains presence for subscribed clients
- Publications sent with `channel="chat:lobby:presence"` for presence updates
