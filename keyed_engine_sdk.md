# Keyed Subscriptions Implementation Plan for centrifuge-js

## Overview

Add keyed subscription support to centrifuge-js SDK, enabling two-phase subscribe protocol with snapshot buffering and presence subscriptions.

## Design Decision: Extend Existing Subscription Class

Rather than creating separate classes, extend the existing `Subscription` class with keyed mode. This maximizes code reuse (state machine, token refresh, reconnection logic) and maintains API consistency.

**Separate constructor for presence subscriptions**: `centrifuge.newPresenceSubscription(channel)` as shown in the design docs.

---

## Implementation Plan

### 1. Update Types (`src/types.ts`)

Add new interfaces and extend existing ones:

```typescript
// Phase constants
export enum KeyedPhase {
  Snapshot = 0,
  Stream = 1,
  Live = 2,
}

// Extended subscription options
export interface SubscriptionOptions {
  // ... existing fields ...

  /** Enable keyed subscription mode */
  keyed?: boolean;
  /** Page size for keyed snapshot/stream pagination (default: 100) */
  keyedLimit?: number;
  /** Request ordered snapshot (sorted by score) */
  keyedOrdered?: boolean;
}

// Keyed publication with key and removed flag
export interface KeyedPublicationContext extends PublicationContext {
  /** The key identifying this entry */
  key?: string;
  /** True if this publication represents a removal */
  removed?: boolean;
}

// Extended subscribed context for keyed subscriptions
export interface SubscribedContext {
  // ... existing fields ...

  /** Snapshot publications for keyed subscriptions */
  publications?: KeyedPublicationContext[];
  /** Server indicates presence is available for this channel */
  presenceAvailable?: boolean;
}
```

### 2. Extend Subscription Class (`src/subscription.ts`)

#### 2.1 Add Private Properties

```typescript
// Keyed subscription state
private _keyed: boolean = false;
private _keyedPresence: boolean = false;  // True for presence subscriptions
private _keyedPhase: 'snapshot' | 'stream' | 'live' | null = null;
private _keyedSnapshotBuffer: any[] = [];  // Buffer snapshot entries
private _keyedStreamBuffer: any[] = [];    // Buffer stream entries during catch-up
private _keyedCursor: string = '';          // Pagination cursor
private _keyedLimit: number = 100;          // Page size
private _keyedOrdered: boolean = false;     // Ordered snapshot
private _keyedPresenceAvailable: boolean = false;  // Server flag
```

#### 2.2 Modify `_setOptions()` Method

Handle new keyed options:
- `keyed` flag
- `keyedLimit`
- `keyedOrdered`

#### 2.3 Modify `_subscribe()` Method

Route to keyed subscribe flow when `_keyed` is true:

```typescript
private _subscribe(): any {
  // ... existing checks ...

  if (this._keyed) {
    return this._keyedSubscribe();
  }
  // ... existing regular subscribe logic ...
}
```

#### 2.4 Add Keyed Subscribe Methods

**`_keyedSubscribe()`** - Entry point for keyed subscriptions:
- Initialize buffers, set phase to 'snapshot'
- Call `_fetchSnapshot()` to start pagination

**`_fetchSnapshot(cursor?: string)`** - Fetch snapshot page:
- Build keyed subscribe command with `phase=SNAPSHOT`, cursor, limit
- Send via `_centrifuge._call()`
- Handle response in `_handleKeyedSnapshotResponse()`

**`_handleKeyedSnapshotResponse(result)`** - Process snapshot response:
- Store epoch/offset from first response
- Validate epoch on subsequent pages (if epoch changed → restart)
- Append publications to `_keyedSnapshotBuffer`
- If `keyed_cursor` present → fetch next page
- If cursor empty → transition to stream or live phase

**`_transitionFromSnapshot()`** - Decide next phase:
- If `_recover` is true and `_offset` exists → start stream phase to catch up
- Otherwise → go directly to live phase

**`_fetchStream()`** - Fetch stream page (offset-based):
- Build command with `phase=STREAM`, offset, epoch, limit
- No cursor for stream - uses offset-based continuation

**`_handleKeyedStreamResponse(result)`** - Process stream response:
- Append publications to `_keyedStreamBuffer`
- Update `_offset` from response
- Continue fetching if more data (based on publications count vs limit)
- When caught up enough → transition to live

**`_requestLive()`** - Final phase, join pub/sub:
- Build command with `phase=LIVE`, offset, epoch
- This triggers server coordination (buffering + subscribe + merge)

**`_handleKeyedLiveResponse(result)`** - Complete keyed subscription:
- Append any remaining publications to `_keyedStreamBuffer`
- Build `SubscribedContext` with `publications: _keyedSnapshotBuffer`
- Emit 'subscribed' event
- Flush `_keyedStreamBuffer` via `_handlePublication()` calls
- Clear buffers, set state to Subscribed

#### 2.5 Build Keyed Subscribe Command

**`_buildKeyedSubscribeCommand(phase, cursor?)`**:

```typescript
private _buildKeyedSubscribeCommand(phase: number, cursor?: string): any {
  const req: any = {
    channel: this.channel,
    keyed: true,
    keyed_phase: phase,
  };

  if (this._token) req.token = this._token;
  if (this._data) req.data = this._data;

  // SNAPSHOT phase
  if (phase === 0) {
    req.keyed_limit = this._keyedLimit;
    if (cursor) req.keyed_cursor = cursor;
    if (this._keyedOrdered) req.keyed_ordered = true;
    // Epoch validation after first page
    if (this._epoch) {
      req.keyed_offset = this._offset;
      req.keyed_epoch = this._epoch;
    }
  }

  // STREAM phase
  if (phase === 1) {
    req.keyed_limit = this._keyedLimit;
    req.keyed_offset = this._offset;
    req.keyed_epoch = this._epoch;
  }

  // LIVE phase
  if (phase === 2) {
    req.keyed_offset = this._offset;
    req.keyed_epoch = this._epoch;
  }

  // Presence flag
  if (this._keyedPresence) {
    req.keyed_presence = true;
  }

  return { subscribe: req };
}
```

#### 2.6 Error Handling

- **Epoch changed during pagination**: Clear buffers, restart from snapshot beginning
- **Concurrent pagination error**: Client-side shouldn't happen (serial requests), but handle gracefully

### 3. Add Presence Subscription Support

#### 3.1 In Centrifuge Class (`src/centrifuge.ts`)

Add `newPresenceSubscription()` method:

```typescript
/** Create a presence subscription for observing who is subscribed to a channel. */
newPresenceSubscription(channel: string, options?: Partial<SubscriptionOptions>): Subscription {
  return this.newSubscription(channel, {
    ...options,
    keyed: true,
    _keyedPresence: true,  // Internal flag
  });
}
```

#### 3.2 In Subscription Class

When `_keyedPresence` is true, set `keyed_presence: true` in subscribe command. Server handles the presence channel transformation internally.

### 4. Update Exports (`src/index.ts`)

```typescript
export { KeyedPhase } from './types';
// KeyedPublicationContext exported via types
```

### 5. Protocol Handling

The keyed subscription uses multiple subscribe requests/responses during the subscription process:

1. **First request**: `phase=SNAPSHOT, cursor="", limit=100`
2. **Pagination**: `phase=SNAPSHOT, cursor="abc...", limit=100`
3. **Stream catch-up**: `phase=STREAM, offset=5000, epoch="xyz", limit=500`
4. **Final join**: `phase=LIVE, offset=9500, epoch="xyz"`

Each request goes through `_centrifuge._call()` and receives a response. The subscription remains in `Subscribing` state throughout, only transitioning to `Subscribed` after the LIVE phase completes.

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/types.ts` | Add `KeyedPhase` enum, `KeyedPublicationContext`, extend `SubscriptionOptions` and `SubscribedContext` |
| `src/subscription.ts` | Add keyed properties, modify `_setOptions`, `_subscribe`, add keyed phase handlers |
| `src/centrifuge.ts` | Add `newPresenceSubscription()` method |
| `src/index.ts` | Export `KeyedPhase` |

---

## Usage Examples

### Basic Keyed Subscription

```typescript
const users = centrifuge.newSubscription('users', {
  keyed: true,
  keyedLimit: 50,
});

users.on('subscribed', (ctx) => {
  // All snapshot entries available here
  console.log('Loaded', ctx.publications.length, 'users');
  ctx.publications.forEach(pub => {
    console.log(pub.key, pub.data);
  });
});

users.on('publication', (ctx) => {
  // Real-time updates (and buffered stream entries)
  if (ctx.removed) {
    console.log('User removed:', ctx.key);
  } else {
    console.log('User updated:', ctx.key, ctx.data);
  }
});

users.subscribe();
```

### Presence Subscription

```typescript
const presence = centrifuge.newPresenceSubscription('chat:lobby');

presence.on('subscribed', (ctx) => {
  console.log('Online users:', ctx.publications.length);
});

presence.on('publication', (ctx) => {
  if (ctx.removed) {
    console.log('User left:', ctx.key);
  } else {
    console.log('User joined:', ctx.key);
  }
});

presence.subscribe();
```

### Keyed with Recovery

```typescript
const leaderboard = centrifuge.newSubscription('leaderboard', {
  keyed: true,
  keyedOrdered: true,
  since: { offset: 5000, epoch: 'abc123' }  // Skip snapshot, start from stream
});
```

---

## Verification

1. **Unit tests**: Add tests for keyed subscription phases, buffering, cursor pagination
2. **Integration tests**:
   - Full two-phase flow (snapshot → stream → live)
   - Presence subscription lifecycle
   - Epoch change handling (restart from beginning)
   - Recovery from position (skip snapshot)
3. **Manual testing**: Test against Centrifuge server with KeyedEngine configured
