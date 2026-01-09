# Current State of Centrifugo and Directions for Improvement

## 1. Current State of Centrifugo

### 1.1 Channel Publications
Centrifugo channels today function primarily as append-only event streams.

**Properties:**
- Publications are transient unless history is enabled.
- Optional history recovery is based on offset + epoch.
- History is bounded by size and TTL.
- Publications are not keyed and do not overwrite previous values.

**Implications:**
- Channels represent events, not state.
- Building materialized state requires replaying history or external storage.
- Pagination of current state is not possible without additional infrastructure.
- Recovery correctness depends on history window sufficiency.

---

### 1.2 Presence

Presence is implemented as a best-effort, ephemeral data structure.

**Implementation characteristics:**
- Redis-backed (hash + sorted set).
- Periodic refreshes from clients.
- TTL-based expiry of stale entries.
- Silent removal on expiry.

**Guarantees:**
- Low-latency joins/leaves in the common case.
- Automatic cleanup of disconnected clients.

**Limitations:**
- No ordering or versioning.
- No replayability.
- No snapshot consistency.
- Unsafe pagination.
- Clients cannot reliably reconcile state after reconnect.

Presence correctness is time-local rather than convergent.

---

## 2. Fundamental Limitations

### 2.1 Publications vs State
Publications model events, while many applications need shared, queryable state. Current approaches rely on:
- History replay
- Client-side deduplication
- External databases

These approaches break down under pagination, partial reads, or high churn.

---

### 2.2 Presence Without Ordering
TTL-based deletion provides no explicit removal events. As a result:
- Snapshot + live updates cannot be reconciled.
- Clients may miss removals permanently.
- Presence may remain incorrect indefinitely after reconnect.

This is an information-theoretic limitation of the model.

---

## 3. Keyed Stateful Channels

Keyed stateful channels extend channels to host a replicated map:
```
key → value
```

**Characteristics:**
- Publications become last-write-wins updates.
- Each update carries a monotonic revision.
- Clients load a paginated snapshot and then apply deltas.
- Snapshot and delta streams are reconciled via revision barriers.

**Problems solved:**
- Snapshot pagination races.
- Infinite history requirements.
- Partial reads.
- Deterministic recovery after reconnect.

Event-only channels remain unchanged.

---

## 4. Converging Membership

Converging membership generalizes presence into an ordered, replayable dataset.

**Key differences from current presence:**

| Current Presence | Converging Membership |
|------------------|----------------------|
| TTL-only expiry  | Explicit LEAVE events |
| Silent removal  | Ordered removals |
| No revisions    | Monotonic revisions |
| Unsafe paging   | Snapshot + delta |
| Time-local      | Eventually convergent |

**Self-healing behavior:**
- Lease expiry produces LEAVE events.
- Network failures and crashes converge automatically.
- Clients can always reconcile membership after reconnect.

---

## 5. Unified Model After Improvements

| Aspect | Before | After |
|------|--------|-------|
| Channel semantics | Event stream | Event stream or replicated state |
| Presence | Ephemeral hint | Converging membership |
| Pagination | Unsafe | Correct |
| Reconnect | Best-effort | Deterministic |
| Snapshot | External | Built-in |
| Correctness | Time-local | Convergent |

---

## 6. Scope Boundaries

These improvements do not introduce:
- Global transactions
- CRDT merging semantics
- Exactly-once delivery
- Infinite retention
- Cross-channel consistency

The system remains simple, scalable, and operationally lightweight.

---

## 7. Implementation on client side

0. Unifying mental model (important)

From the client’s perspective, both features are the same abstraction:

A channel exposes

a keyed snapshot at some revision

a log of changes after that revision

revisions are (epoch, offset)

The only difference:

Stateful channel → keys are arbitrary state keys, payload is user-defined

Presence → keys are client IDs, payload is ClientInfo

Presence has extra server-side rules (TTL, late leave), but the client reading logic is identical

That’s the key insight.

1. Data model exposed to the client
   Snapshot entry

Each snapshot entry MUST carry:

key
payload
revision = (epoch, offset)


Revision is mandatory.
Payload can be opaque bytes (not JSON).

Publication (delta or full)

Each publication carries:

type: upsert | remove
key
payload?        (absent for remove)
revision = (epoch, offset)

2. Client local state

The client keeps:

local_snapshot: Map<Key, { payload, revision }>
last_seen_revision: (epoch, offset)


This is per subscription.

3. Subscribe flow (both stateful & presence)
   Step 1: Subscribe request

Client sends:

SUBSCRIBE channel


Server responds with:

SUBSCRIBE_OK {
snapshot_revision: (epoch, offset)
}


This revision is the cut point: snapshot is valid up to this revision.

Step 2: Snapshot pagination (IMPORTANT)

Client requests snapshot pages:

SNAPSHOT_REQUEST {
channel,
since_revision: snapshot_revision,
limit,
cursor
}


Server returns:

SNAPSHOT_REPLY {
entries: [
{ key, payload, revision }
],
snapshot_revision,
next_cursor | eof
}

Client rules while reading snapshot

For each entry:

if entry.revision <= snapshot_revision:
local_snapshot[key] = entry
else:
ignore (belongs to newer epoch / race)


When eof:

local last_seen_revision = snapshot_revision


⚠️ Critical rule
If during pagination the server reports a different snapshot_revision, client MUST:

discard snapshot
restart from beginning


This is how races are eliminated.

4. Transition to live publications

Once snapshot is fully read:

Client is already receiving publications via subscription push.

For each publication:

if publication.revision <= last_seen_revision:
ignore (duplicate / replay)
else:
apply publication
last_seen_revision = publication.revision

5. Applying publications (core logic)
   Upsert event
   existing = local_snapshot[key]

if existing == nil OR publication.revision > existing.revision:
local_snapshot[key] = {
payload,
revision
}

Remove event
existing = local_snapshot[key]

if existing != nil AND publication.revision >= existing.revision:
delete local_snapshot[key]


This is CRDT-like last-write-wins, but totally deterministic because revisions are monotonic.

6. What happens if leave was missed? (presence-specific, but client logic unchanged)

Scenario:

client A disappears

leave was not delivered

Redis TTL cleanup later emits a synthetic leave with revision (e, o)

Client behavior:

receives REMOVE with higher revision

applies removal

state converges

No special handling required client-side.

7. Handling reconnects / recoverable subscriptions

On reconnect, client sends:

SUBSCRIBE channel {
recover: true,
last_seen_revision
}


Server behavior:

if log covers last_seen_revision → stream publications only

else → force snapshot resend

Client logic is unchanged:

snapshot path or publication-only path both converge to same state

8. Presence-specific notes (what’s different)
   What the client sees

Presence is just:

Map<ClientID, ClientInfo>

What the client does NOT care about

user_id aggregation

TTL expiration

synthetic leave generation

connection counting

Those are server-side concerns only.

Client logic:

read snapshot

apply upserts

apply removes

trust revisions

9. Why revision checks MUST be client-side

You asked this earlier — now it should be clear:

Pagination + concurrent updates cannot be made race-free server-side alone

Client must:

remember snapshot revision

reject out-of-range entries

ignore stale publications

This is exactly what Kafka consumers do.

10. Properties the Lua script MUST guarantee

For the client logic above to be correct, Lua must guarantee:

Every snapshot entry has a revision

Every publication has a revision

Revisions are totally ordered per channel/shard

Remove events carry a revision

Snapshot revision is stable during pagination

Epoch change invalidates old snapshots

Your current Lua script already satisfies ~90% of this.

11. Stateful channels vs converging presence (summary table)
    Aspect	Stateful channel	Presence
    Key	arbitrary string	client ID
    Payload	opaque bytes	ClientInfo
    Remove	explicit	explicit + TTL
    Snapshot	yes	yes
    Publication	yes	yes
    Client logic	identical	identical
    Server aggregation	optional	user count
    TTL cleanup	optional	required