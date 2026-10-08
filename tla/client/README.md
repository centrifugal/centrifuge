# TLA+ model of the subscription protocol

`Subscriptions.tla` models one client connection of Centrifuge together with
the SDK at the other end of the connection, the broker and hub, and the
application's handlers. TLC checks it exhaustively for small bounds: handler
calls (SubscribeHandler, UnsubscribeHandler, ConnectHandler, DisconnectHandler),
the frames written to the connection, recovery and the publication buffer
(`internal/recovery`), presence and join/leave, under all interleavings of
asynchronous SubscribeCallbacks, server-side Subscribe/Unsubscribe calls,
publications and disconnects.

| File | What |
|---|---|
| `Subscriptions.tla` | the spec (plain TLA+) |
| `gen_configs.py` | generates `configs/*.cfg` (`python3 tla/client/gen_configs.py`); change the configs there, not the `.cfg` files |
| `configs/MC_*.cfg` | configurations of the current code, each must pass |
| `configs/bug_*.cfg` | old behaviours (`Bugs`), each must fail |
| `spec.conf` | the main module and the default set of `make tla` |

## How to run

See `tla/README.md`. For this spec:

```sh
make tla SPEC=client                               # default set, a few minutes
make tla SPEC=client CONFIGS=all                   # every config, about 30 minutes
make tla SPEC=client CONFIGS="MC_small bug_no_join_gate"
sh tla/run.sh client bug_no_join_gate              # prints the counterexample's actions
```

The full TLC output of a config is in `tla/out/client/<config>.txt`. The safety
configs use `VIEW View`, which leaves the `act` ghost (the name of the last
action, shown in traces) out of the state fingerprint; the liveness configs
don't use it.

## What is modelled

One connection, channels `c1` (and `c2` in `MC_two_channels`). Every goroutine
of the Go code which takes part is a process in `pr` (id -> record with a `pc`),
and every action is named after the Go function or code block it mirrors.
Steps are as coarse as the critical sections of the Go code (`c.mu`, the hub
shard lock, the buffer lock), and split where the Go code does something outside
of them that another goroutine can interleave with (e.g. a write to the
connection after a lock is released).

| Process | Go | Notes |
|---|---|---|
| SDK (`SDK_*`) | centrifuge-js | sends subscribe/unsubscribe commands (bounded by `MaxCmds`), reads frames in order; an unsubscribe push acts only on a subscribed subscription, code 2500 (insufficient state) resubscribes with recovery from its position; publications only count when subscribed |
| `<<"rd",0>>` reader | `connectCmd`, `triggerConnect`/`connect`, `HandleCommand` | processes commands one at a time; `handleSubscribe` returns right after calling SubscribeHandler |
| `<<"cb",g>>` | the application's `SubscribeCallback` of attempt `g` + the rest of `handleSubscribe`'s cb | invoked by the application **at any time** from its own goroutine, or never (no fairness), with allow / deny / refuse (allowed, then refused by Centrifuge, e.g. expired) / `DisconnectConnectionClosed`; possibly twice (`CB_SecondInvocation`, the `answered` guard). Interleaves with later commands, other channels' callbacks, unsubscribes, close and DisconnectHandler |
| `<<"ae",g>>` | `endSubscribeAttempt`'s goroutine | UnsubscribeHandler(Subscribed=false) of an ended attempt, then `finishAttemptEnd` |
| `<<"su",i>>` | `Client.Unsubscribe` (Node.Unsubscribe) | from an application goroutine at any time, deferred while connecting |
| `<<"ss",i>>` | `Client.Subscribe` | on a connected client |
| `<<"ins",i>>` | `handleInsufficientState` -> `handleAsyncUnsubscribe(ch, 0, insufficient)` | client-side; server-side ones close the client |
| `<<"close",0>>` | `close()` | spawned by the transport closing (`TransportClose`) or by error/timeout paths (`go c.close(...)`) |
| broker | `Publish`, PUB/SUB -> `hub.broadcastPublication` -> `writePublication` | offsets assigned in order; broadcast is atomic with respect to `node.removeSubscription` (hub shard lock); `Lossy`: PUB/SUB may drop one (history keeps it); offsetless publications |

Shared steps used by several processes:

| Actions | Go |
|---|---|
| `UW_Snapshot`, `UW_SubscribingChClosed`, `UW_SubscribeWaitTimeout`, `UW_Remove`, `UW_EndAttemptAndLeave`, `UW_RemoveFromHub`, `UW_SendUnsubscribe`, `UW_UnsubscribeHandler`, `UW_WaitOtherUnsubscribe`, `UW_WaitOtherTimeout` | `unsubscribeWaiting` (client unsubscribe, server unsubscribe with push, insufficient state, close's loop): snapshot, wait for a subscribe in progress, identity-matched removal with `addAttemptEndLocked`/`addPendingUnsubscribeLocked`/`addPendingLeaveLocked` and `joinGate.leave`, `endSubscribeAttempt`, `removePresenceAndLeave`, `removeSubscription`, `sendUnsubscribe` + `finishPendingLeave`, UnsubscribeHandler + deferred `finishAttemptEnd`/`finishPendingUnsubscribe`, `waitOtherUnsubscribe` |
| `SC_StartBufferingAndAddSubscription`, `SC_ReadHistory`, `SC_ReadBuffered`, `SC_WriteReply`, `SC_CommitSubscription`, `SB_StopBuffering`, `SC_CloseSubscribingCh`, `JoinAndFinishJoin` | `subscribeCmd` (StartBuffering or StartQueueing, reservation checks, `addSubscription`, `addPresence`, history read, `ReadBuffered` + `MergePublications`, reply), `commitSubscription`, `StopBuffering(writePendingPublication)` (check and write under `c.mu`), deferred `close(subscribingCh)`, `publishJoinAndPresence` + `finishJoin` (`joinGate.join`) |
| `CB_SubscribeCallback` | `answerSubscribeCallback` + `onSubscribeErrorGen` + error reply |
| `RD_HandleSubscribe`, `RD_WaitAttemptEnds`, `RD_WaitAttemptEndsTimeout` | `validateSubscribeRequest` (reservation, `mustWaitAttemptEndsLocked`) + SubscribeHandler call, `waitAttemptEnds` |
| `RD_ConnectCmd` ... `RD_ConnectRelease` | connect-time subscription in `connectCmd` (reserve, subscribeCmd server-side, connect reply, take over the reservation, StopBuffering, close subscribingCh, rollback of lost/closed) |
| `RD_ConnectHandlerStart/End`, `RD_UnsubscribeAfterConnect` | `connect()` (connectMu, ConnectHandler registers OnUnsubscribe at its end, statusConnected) and `triggerConnect` applying `unsubscribesAfterConnect` |
| `SS_Reserve`, `SS_WriteSubscribePush`, `SS_CommitSubscription` | `Client.Subscribe` (wait for pending leave, reserve, subscribeCmd, writeSubscribePush, commit, StopBuffering, close subscribingCh, join) |
| `CL_Start`, `CL_UnsubscribeLoop`, `CL_DisconnectHandler` | `close()` (connectMu, statusClosed, snapshot, disconnect push and writer closed, unsubscribe loop, `waitPendingUnsubscribes`, DisconnectHandler if it was connected) |

## What is abstracted away

- **Timeouts** are nondeterministic give-up steps where the Go code has bounded
  waits: the 5 s wait for a subscribe in progress (`UW_SubscribeWaitTimeout`),
  `waitOtherUnsubscribe`, the 5 s `waitAttemptEnds`, the pending-leave wait of
  `Client.Subscribe`, and the 30 s `waitPendingUnsubscribes`. They are enabled
  only with `Timeouts = TRUE` (`MC_timeouts`), where the documented exceptions
  apply and the ordering properties are not checked. One exception: close()
  may always give up waiting for an attempt whose SubscribeCallback was not
  invoked yet, since a callback may never be invoked (otherwise close would block
  forever); the documented "late attempt end after DisconnectHandler" is then
  allowed by `NothingAfterDisconnectHandler`.
- Map subscriptions, shared poll, presence ticks (`updatePresence`,
  `compensateRacedPresence`), subscription expiration/refresh, server tags
  filters, delta, channel compaction, `ClientChannelLimit` and
  `maxPendingAttemptEnds` (16), history retention (recovery always
  succeeds), buffer overflow (`ClientQueueMaxSize`) and lag: not modelled.
- The connection writer is one FIFO; the per-channel batch writer
  (`perChannelWriter`, `delWriter`) is not modelled.
- Publications of a channel are broadcast one at a time in offset order.
- Presence is one entry per channel per client (as in the presence manager:
  keyed by client id); join/leave are recorded in order of publishing.
- The SDK never reconnects: after the disconnect push it stops. Client-side and
  server-side subscriptions use different channels (`ClientChans`,
  `ServerChans`): centrifuge-js drops publications of a server-side
  subscription to a channel it also has a client-side Subscription object for.
- `Client.Subscribe` is only called on a connected client.
- Two connections of one user are not modelled: their state is independent in
  this part of the code (user-level presence is not modelled).
- Epochs are not modelled, so `MergePublications` always gets a read epoch.
- Some consecutive Go steps are merged where no other goroutine's step can make
  a difference in between (documented at the actions), e.g. StartBuffering +
  checks + `addSubscription` + `addPresence`; the rollback in
  `onSubscribeErrorGen`; close's disconnect push and writer close.

## Properties

Invariants (`viol`/`mon.bad` are ghost sets recorded by the actions):

| Property | Meaning |
|---|---|
| `AtMostOneUnsubscribeHandler` | never more than one UnsubscribeHandler call per allowed attempt or server-side subscription |
| `ConsistentAtQuiescence` | when nothing is in flight: SDK view = server `c.channels` = hub (and positions equal), no buffers, no pending tracking, every allowed attempt that is not live got exactly one UnsubscribeHandler call, every ended server-side subscription got one (once ConnectHandler started), presence iff subscribed, joined iff not live <=> left |
| `ReleasedAtQuiescence` | the handler-call and tracking part of `ConsistentAtQuiescence`, used with `Timeouts` |
| `CleanAfterClose` | after close, once every goroutine finished: nothing in hub/buffers/presence/c.channels, every allowed attempt got exactly one call, every join has its leave |
| `UnsubscribeBeforeNextSubscribe` | UnsubscribeHandler of an attempt/subscription of a channel comes before the SubscribeHandler of the next attempt of the channel |
| `NothingAfterDisconnectHandler` | no handler call after DisconnectHandler, except the attempt end of an attempt whose SubscribeCallback was invoked after it |
| `NoUnsubscribeHandlerDuringConnectHandler` | no UnsubscribeHandler call (or missed call) while ConnectHandler runs |
| `LeaveAfterJoin` | a subscription's leave comes after its join |
| `JoinAfterPreviousLeave` | the next subscription's join comes after the previous subscription's leave |
| `NoPublicationOutsideSubscription` | on the wire, per channel: no publication before the subscribe result / subscribe push / connect result, or after the unsubscribe push or reply |
| `ContiguousOffsets` | positioned/recovering: recovered and live publications follow the result's offset without gap or duplicate |
| `NoUnsubscribePushAfterLaterResult` | no unsubscribe push of a subscription after the result of a later subscription of the channel |
| deadlock | any state without successor other than `AllDone` (a goroutine blocked forever) is reported by TLC |

Liveness (`SpecAnswering`: weak fairness on every library goroutine, the SDK
reading and the broker broadcasting; the application eventually invokes every
SubscribeCallback; publishers, the SDK user, timers are not fair):

| Property | Meaning |
|---|---|
| `AllowedAttemptReleased` | every allowed attempt eventually gets its UnsubscribeHandler call, or is a live subscription |
| `SubscribeCommandReplied` | every command sent eventually gets its reply (or the connection is closed) |
| `PublicationDelivered` | a publication published while the subscription is live (server and SDK) eventually reaches the SDK's position, unless it ends |

## Bug variants

`Bugs` is a set of old behaviours to put back, `{}` for the current code. Each
`bug_*` config must fail; it shows that the property it breaks is checked.

| Config | `Bugs` | Old behaviour | Fails |
|---|---|---|---|
| `bug_push_after_handler` | `PushAfterHandler` | a server unsubscribe sent its unsubscribe push after UnsubscribeHandler, without a pending leave, attempt-end tracking or `waitOtherUnsubscribe` | `UnsubscribeBeforeNextSubscribe`: the client resubscribes before UnsubscribeHandler(1) and SubscribeHandler(2) is called first |
| `bug_push_after_handler_wire` | `PushAfterHandler` | same, wire properties only | `NoUnsubscribePushAfterLaterResult`: the push of subscription 1 is written after the reply of subscription 2 |
| `bug_server_sub_early_release` | `ServerSubEarlyRelease`, `PendingWriteUnlocked` | `Client.Subscribe` released a waiting unsubscribe right after the commit, before StopBuffering (only visible while queued publications were also written outside `c.mu`) | `NoPublicationOutsideSubscription`: a queued publication after the unsubscribe push |
| `bug_no_attempt_end_call` | `NoAttemptEndCall` | no UnsubscribeHandler for allowed attempts which never became subscriptions | `ConsistentAtQuiescence` |
| `bug_no_attempt_end_call_liveness` | `NoAttemptEndCall` | same | liveness `AllowedAttemptReleased` |
| `bug_no_attempt_end_wait` | `NoAttemptEndWait` | a subscribe didn't wait for the asynchronous UnsubscribeHandler of an ended attempt of the channel | `UnsubscribeBeforeNextSubscribe` |
| `bug_no_join_gate` | `NoJoinGate` | an unsubscribe between the commit and the join published the leave first | `LeaveAfterJoin` |
| `bug_server_unsub_not_ordered` | `ServerUnsubNotOrdered` | a server unsubscribe of a live subscription was not tracked as an attempt end | `UnsubscribeBeforeNextSubscribe` |
| `bug_no_connect_defer` | `NoConnectDefer` | `Client.Unsubscribe` while connecting was not deferred until ConnectHandler returned | `NoUnsubscribeHandlerDuringConnectHandler` |
| `bug_pending_write_unlocked` | `PendingWriteUnlocked` | see below, 1 | `NoPublicationOutsideSubscription` |
| `bug_pending_write_unlocked_duplicate` | `PendingWriteUnlocked` | see below, 1 | `ContiguousOffsets` (duplicate after a resubscribe) |
| `bug_pending_write_unlocked_server` | `PendingWriteUnlocked` | see below, 1 (`Client.Subscribe`) | `NoPublicationOutsideSubscription` |
| `bug_merge_no_gap_check` | `MergeNoGapCheck` | see below, 2 | `ContiguousOffsets` |
| `bug_no_wait_other_after_subscribe` | `NoWaitOtherAfterSubscribe` | see below, 3 | `NoPublicationOutsideSubscription` |
| `bug_offsetless_before_result` | `OffsetlessBeforeResult` | see below, 4 | `NoPublicationOutsideSubscription` |

## Bugs found by the model

1. **A queued publication after the unsubscribe** (`PendingWriteUnlocked`).
   StopBuffering runs after the subscription is committed, so an unsubscribe
   arriving after the commit doesn't wait for it. `writePendingPublication`
   checked the subscription and advanced its position under `c.mu`, but wrote
   the publication after releasing it. An unsubscribe could run entirely in
   between: the publication came after the unsubscribe reply or push, and after
   a resubscribe it came again after the new subscription's result (a
   duplicate). Fix: `writePendingPublication` checks and writes under `c.mu`.
2. **Recovery skipping a publication lost by PUB/SUB** (`MergeNoGapCheck`).
   `MergePublications` only compared neighbouring publications. When a client
   recovered from the top of the stream and PUB/SUB lost the next publication
   while a later one was buffered, the result was "recovered" with a gap, and
   the position moved past the lost publication. Fix: `MergePublications` gets
   the read position and fails (insufficient state) unless the merged offsets
   after it continue it without a gap.
3. **An unsubscribe reply before another unsubscribe's push**
   (`NoWaitOtherAfterSubscribe`). A client unsubscribe which waited for a
   subscribe in progress and then found the channel gone replied at once,
   before the push of the unsubscribe which removed it; publications without
   offset could follow its reply. Fix: it calls `waitOtherUnsubscribe` there
   too, as when it finds nothing to remove.
4. **Publications without offset before the subscribe result**
   (`OffsetlessBeforeResult`). A subscription without positioning or recovery
   had no buffer, so once in the hub it got publications without offset before
   its result was written. Fix: such a subscription starts its buffer in the
   queueing phase (`StartQueueing`).

## Results

The configs, with the time of a run on a 10-core laptop (TLC 2.19). The
default set of `make tla` is marked; `all` takes about 33 minutes.

| Config | Bounds | Distinct states | Time | Default set |
|---|---|---|---|---|
| `MC_small` | c1 client-side; 3 commands, 1 publication, 1 offsetless, 1 Client.Unsubscribe, close | 4,351,210 | 82s | yes |
| `MC_not_positioned` | as small without positioning/recovery, 2 publications | 7,658,794 | 111s | yes |
| `MC_double_invoke` | as small, no publications, SubscribeCallback may be invoked twice | 980,575 | 15s | yes |
| `MC_liveness` | liveness, 2 commands, 1 publication, no close | 41,098 | 11s | yes |
| `MC_liveness_close` | liveness, 2 commands, close | 112,992 | 23s | yes |
| `MC_large` | as small with 2 publications (lossy PUB/SUB), join/leave and presence, insufficient state | 23,280,388 | 341s | |
| `MC_two_channels` | c1 and c2, 3 commands, 1 Client.Unsubscribe, close | 18,965,682 | 307s | |
| `MC_server` | server-side: 2 Client.Subscribe, 2 Client.Unsubscribe, 2 publications, join/leave, close | 26,884,451 | 469s | |
| `MC_connect` | connect-time subscription, 1 Client.Subscribe, 2 Client.Unsubscribe (also while connecting), join/leave, close | 7,028,423 | 115s | |
| `MC_timeouts` | as small with 1 publication, every bounded wait may time out; reduced invariant set | 27,239,703 | 449s | |
| `bug_*` (15 configs) | each fails in 0-4s, `bug_push_after_handler_wire` in 23s | | 52s in total | yes |

`MC_timeouts` checks `AtMostOneUnsubscribeHandler`,
`NoUnsubscribeHandlerDuringConnectHandler`, `LeaveAfterJoin`,
`ReleasedAtQuiescence` and `CleanAfterClose`. The other properties fail only
through the documented exceptions of the 5 s and 30 s waits. Two of them are
worth knowing: when a resubscribe stops waiting for the previous subscription's
unsubscribe push, the push can follow the new subscription's reply and the SDK
ends the new subscription while the server keeps it; and with join/leave, the
old subscription's presence removal (by client id) can remove the new one's
presence.
