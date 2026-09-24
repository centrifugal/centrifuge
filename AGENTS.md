# AGENTS.md

## Recovery sync invariant (`internal/recovery/sync.go`)

Publications are broadcast under a hub shard lock, so a broadcast must never wait for a subscribing client.

For a subscription with positioning or recovery (stream and map paths):

1. `StartBuffering` before `addSubscription`, keep the returned `*Buffer`.
2. `ReadBuffered` after reading history/stream, with the position read (it leaves out publications the read covers). If it reports that its publications can't be merged (another epoch, or over the limit), fail with insufficient state; otherwise merge them into the result.
3. Commit, write the result, then `StopBuffering`. It writes the publications queued meanwhile; if it returns true, resubscribe the client with insufficient state.
4. Every other exit calls `CancelBuffering`. A leaked buffer queues the channel's publications forever.

Publications without offset can't be synced: they are dropped until `StopBuffering` has written the queue.

All buffers of a client share one limit, `ClientQueueMaxSize`, counted separately from its write queue: it bounds what a connection holds while it subscribes, however many channels it subscribes to.

Nothing of the channel goes to the client before its result, and nothing of an unsubscribe before the result and recovered publications. Recovered publications are always delivered.

Guarded by `TestClientRecovery*`, `TestClientRecoveringSubscribeFailureCancelsBuffering`, the leak check in `TestMain` and `internal/recovery` tests.
