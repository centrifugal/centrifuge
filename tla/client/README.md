# TLA+ model of a client connection

`Client.tla` models one client connection of Centrifuge together with the SDK
at the other end of the connection, the broker and hub, and the application's
handlers. TLC checks it exhaustively for small bounds: the connection
lifecycle (connect, ConnectHandler, concurrent closes, expiry and refresh, the
stale timer), subscriptions (handler calls, the frames written to the
connection, recovery and the publication buffer of `internal/recovery`
including epochs, lagging reads and overflow), presence ticks, join/leave and
subscription expiry and refresh, under all interleavings of asynchronous
callbacks, server-side Subscribe/Unsubscribe/Disconnect/Refresh calls,
publications and timers.

| File | What |
|---|---|
| `Client.tla` | the main module: subscribe/unsubscribe, the reader, callbacks, broker, SDK, next-state relation, properties |
| `ClientBase.tla` | constants, variables, initial state, helpers, the wire monitor |
| `ClientConnection.tla` | close(), connection expiry and refresh, the stale timer, Client.Disconnect, slow writes |
| `ClientPresence.tla` | presence ticks, subscription expiry and sub refresh |
| `gen_configs.py` | generates `configs/*.cfg` (`python3 tla/client/gen_configs.py`); change the configs there, not the `.cfg` files |
| `configs/MC_*.cfg` | configurations of the current code, each must pass |
| `configs/bug_*.cfg` | old behaviours (`Bugs`), each must fail |
| `spec.conf` | the main module and the default set of `make tla` |

## How to run

See `tla/README.md`. For this spec:

```sh
make tla SPEC=client                               # default set, about 7 minutes
make tla SPEC=client CONFIGS=all                   # every config, about 3 hours
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
| `<<"close",reason>>` | `close()` | one per reason, several may run: the transport closing (`TransportClose`, reason `closed`), `Client.Disconnect` (`force`), a slow queue through `spawnCloseUnlessClosing` (`slow`, only when `closing` is not set), expiry (`expired`, `subexpired`), the stale timer (`stale`) and error/timeout paths (`error`, `insufficient`). The first to take `connectMu` with the client not closed does the work |
| `<<"exp",0>>` | `onTimerOp(timerOpExpire)` -> `expire()` | the expire timer fires once `c.exp` passed; RefreshHandler (server-side refresh) or `checkExpired` |
| `<<"rcb",n>>` | the application's `RefreshCallback` | of the expire timer's RefreshHandler call or of a client `refresh` command; at any time or never; extend / expired / error |
| `<<"aref",n>>` | `Client.Refresh` | extend, expired or no expiry, on a connected client |
| `<<"stale",0>>` | `closeStale` | the stale timer of a new connection |
| `<<"tick",0>>` | `updatePresence` | one presence tick at a time (`presenceInFlight`), under `presenceMu`: snapshot, presence adds, subscription expiry, `compensateRacedPresence` |
| `<<"cras",c>>` | `compensateRacedPresenceAfterSubscribe` | a tick's presence add racing a resubscribe in progress, decided after it |
| `<<"srcb",n>>` | the application's `SubRefreshCallback` | of the tick (server-side subscription refresh) or of a `sub_refresh` command |
| `<<"xu",g>>` | `handleAsyncUnsubscribe(ch, subGen, unsubscribeExpired)` | an expiry unsubscribe of subscription `g` |
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
| `CL_StartClosing`, `CL_SetClosed`, `CL_WriteDisconnectPush`, `CL_PresenceLock`, `CL_UnsubscribeLoop`, `CL_DisconnectHandler` | `close()`: `closing`, `connectMu`, statusClosed (or return), stop the timers, snapshot; the disconnect push written as the last frame with the queue closed (not for the transport closing; dropped for a slow queue); `presenceMu`; unsubscribe loop; `waitPendingUnsubscribes`; DisconnectHandler if it was connected |
| `RD_ScheduleOnConnectTimers` | `scheduleOnConnectTimers`: expire and presence timers, the stale timer stopped |
| `RD_HandleRefresh`, `RD_HandleSubRefresh` | `handleRefresh`, `handleSubRefresh`: RefreshHandler / SubRefreshHandler with the callback on its own goroutine |
| `ExpireTimerFires`, `EXP_*`, `RCB_*`, `CHK_*` | `expire()`, the RefreshCallback (c.exp, timer re-armed with `addExpireUpdate`), `checkExpired` |
| `T_Snapshot`, `T_UpdateChannelPresence`, `T_AddPresence`, `T_CheckSubscriptionExpiration`, `T_CompensateRacedPresence`, `T_RemoveRacedPresence`, `CRAS_*` | `updatePresence`, `updateChannelPresence` (check, then the add), `checkSubscriptionExpiration`, `compensateRacedPresence` with `tickPresenceToRemove` and `addPendingLeaveLocked`, `removeRacedPresence` |
| `SRCB_*` | SubRefreshCallback: gen-matched write-back of `expireAt`, expiry unsubscribe of the decided subscription, `DisconnectSubExpired` |
| `WritePublication`, `LiveOutcome` | `writePublication`: `SyncPublication` (collect / queue up to `BufferLimit`), `writePublicationUpdatePosition` (empty epoch adopts the publication's, epoch mismatch or gap: insufficient state, older: dropped) |

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
- Time is abstract: the connection's and subscriptions' expiry are a state
  (`valid`, `expired`, none) changed by `TimeExpires`/`SubTimeExpires`; timers
  fire nondeterministically once armed (the expire timer only once `c.exp`
  passed). Ping/pong, the alive handler and `unusable` are not modelled.
- Map subscriptions, shared poll, server tags filters, delta, channel
  compaction, `ClientChannelLimit`, `maxPendingAttemptEnds` (16), the periodic
  position check and lag (`maxLagExceeded`) are not modelled. History
  retention is `HistorySize` publications, `ClientQueueMaxSize` a number of
  publications (`BufferLimit`).
- A stream reset (`EpochReset`, a new epoch with offsets from 1) only happens
  when every publication of the old epoch was broadcast.
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
- Some consecutive Go steps are merged where no other goroutine's step can make
  a difference in between (documented at the actions), e.g. StartBuffering +
  checks + `addSubscription` + `addPresence`; the rollback in
  `onSubscribeErrorGen`.
- Presence ticks bound the number of SubRefreshHandler calls from ticks
  (`MaxTicks`); RefreshCallback answers which extend the connection are bounded
  by `MaxExtends`, so the expire timer can fire again and again.

## Properties

Invariants (`viol`/`mon.bad` are ghost sets recorded by the actions):

| Property | Meaning |
|---|---|
| `AtMostOneUnsubscribeHandler` | never more than one UnsubscribeHandler call per allowed attempt or server-side subscription |
| `ConsistentAtQuiescence` | when nothing is in flight: SDK view = server `c.channels` = hub (and positions equal), no buffers, no pending tracking, every allowed attempt that is not live got exactly one UnsubscribeHandler call, every ended server-side subscription got one (once ConnectHandler started), presence iff subscribed, joined iff not live <=> left |
| `ReleasedAtQuiescence` | the handler-call and tracking part of `ConsistentAtQuiescence`, used with `Timeouts` |
| `CleanAfterClose` | after close, once every goroutine finished: nothing in hub/buffers/presence/c.channels, every allowed attempt got exactly one call, every join has its leave |
| `UnsubscribeBeforeNextSubscribe` | UnsubscribeHandler of an attempt/subscription of a channel comes before the SubscribeHandler of the next attempt of the channel |
| `NothingAfterDisconnectHandler` | no UnsubscribeHandler or SubscribeHandler call after DisconnectHandler, except the attempt end of an attempt whose SubscribeCallback was invoked after it |
| `NoUnsubscribeHandlerDuringConnectHandler` | no UnsubscribeHandler call (or missed call) while ConnectHandler runs |
| `LeaveAfterJoin` | a subscription's leave comes after its join |
| `JoinAfterPreviousLeave` | the next subscription's join comes after the previous subscription's leave |
| `NoPublicationOutsideSubscription` | on the wire, per channel: no publication before the subscribe result / subscribe push / connect result, or after the unsubscribe push or reply |
| `ContiguousOffsets` | positioned/recovering: recovered and live publications follow the result's offset without gap or duplicate |
| `NoUnsubscribePushAfterLaterResult` | no unsubscribe push of a subscription after the result of a later subscription of the channel |
| `DisconnectHandlerOnce` | DisconnectHandler at most once, and, once everything is done, exactly once if ConnectHandler was called |
| `DisconnectFrameOfWinner` | the disconnect push carries the reason of the close that took `connectMu` first |
| `NothingAfterDisconnectFrame` | nothing is written after the disconnect push |
| `NoExpireTimerOnClosedClient` | a refresh never re-arms the expire timer of a closed client |
| `NoRefreshOrExpiryOfOtherSubscription` | a SubRefreshCallback only extends the subscription it was called for, an expiry unsubscribe only removes the subscription it was decided for (subGen identity) |
| `NoPublicationOfAnotherEpochDropped` | a publication is dropped as already delivered only when the subscription's position is of the publication's epoch |
| deadlock | any state without successor other than `AllDone` (a goroutine blocked forever) is reported by TLC |

The wire monitor also checks that a publication is not of another epoch than
the subscription's (`PublicationOfAnotherEpoch`), and that an empty result
epoch is not adopted by a publication of another epoch than the one the
position belongs to (`EpochAdoptedOverAnotherEpoch`); both are part of
`ContiguousOffsets`' monitor set.

Handler calls after DisconnectHandler: the library promises only that
UnsubscribeHandler calls (with the documented exception) and SubscribeHandler
calls come before DisconnectHandler. Command handlers (RPC, publish, refresh,
sub_refresh and so on) and the expire timer's RefreshHandler may run
concurrently with DisconnectHandler or after it, and the model allows that.

Liveness (`SpecAnswering`: weak fairness on every library goroutine, the SDK
reading and the broker broadcasting; the application eventually invokes every
SubscribeCallback; publishers, the SDK user, timers are not fair):

| Property | Meaning |
|---|---|
| `AllowedAttemptReleased` | every allowed attempt eventually gets its UnsubscribeHandler call, or is a live subscription |
| `SubscribeCommandReplied` | every command sent eventually gets its reply (or the connection is closed) |
| `PublicationDelivered` | a publication published while the subscription is live (server and SDK) eventually reaches the SDK's position, unless it ends |
| `ExpiredConnectionClosed` | a connection whose credentials stay expired is eventually closed (`MC_liveness_expiry`) |

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
| `bug_disconnect_push_not_last` | `DisconnectPushNotLast` | see below, 5 | `NothingAfterDisconnectFrame` |
| `bug_empty_epoch_kept_after_merge` | `EmptyEpochKeptAfterMerge` | see below, 6 | `NoPublicationOfAnotherEpochDropped` |
| `bug_cras_single_wait` | `CrasSingleWait` | see below, 7 | `ConsistentAtQuiescence` (presence of a live subscription missing) |
| `bug_tick_remove_no_pending_leave` | `TickRemoveNoPendingLeave` | a presence tick removed the presence its add raced an unsubscribe with, without a pending leave: a resubscribe could add its presence first and lose it | `ConsistentAtQuiescence` (presence of a live subscription missing) |
| `bug_tick_remove_ignores_subscription` | `TickRemoveIgnoresSubscription` | the tick removed its raced presence even when a resubscribe had added the same presence, or waited for none in progress | `ConsistentAtQuiescence` |
| `bug_sub_refresh_no_gen_check` | `SubRefreshNoGenCheck` | a SubRefreshCallback extended whatever subscription the channel had by then | `NoRefreshOrExpiryOfOtherSubscription` |
| `bug_async_unsubscribe_any_gen` | `AsyncUnsubscribeAnyGen` | an expiry unsubscribe removed whatever subscription the channel had by then | `NoRefreshOrExpiryOfOtherSubscription` |

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
5. **Frames after the disconnect push** (`DisconnectPushNotLast`). close()
   enqueued the disconnect push and only then closed the writer: anything
   enqueued in between (a broadcast publication, a subscribe reply, the
   connect reply) was written after the push. Fix: the per-channel writer is
   closed first, and the push is written through a writer method which closes
   the queue and writes the remaining items followed by the push as the last
   frame; later writes are dropped.
6. **A publication dropped after a subscribe which read an empty stream**
   (`EmptyEpochKeptAfterMerge`). A subscribe read the stream position from a
   lagging replica: offset 0, no epoch. A publication buffered meanwhile was
   merged (with an empty read epoch `ReadBuffered` and `MergePublications`
   only compare the buffered publications among themselves), so the
   subscription's position became its offset with an empty epoch. If the
   stream was then reset, the first publication of the new epoch adopted the
   empty epoch and, its offset being at or below the position, was dropped as
   already delivered; the next ones were delivered as if contiguous. Fix: after
   the merge, an empty read epoch takes the epoch of the merged buffered
   publications (`CollectedEpoch`), for the result and the stored position.
7. **A presence tick removing the presence of a later resubscribe**
   (`CrasSingleWait`). A tick's presence add raced an unsubscribe and a
   resubscribe (attempt 2), so `compensateRacedPresenceAfterSubscribe` waited
   for attempt 2. If attempt 2 failed and the client subscribed again (attempt
   3), the woken goroutine took attempt 3's reservation, not subscribed yet,
   for no live subscription, registered a pending leave too late for attempt 3
   and removed the presence attempt 3 had added meanwhile under the same client
   key: attempt 3 was live without presence until the next tick. Fix: while the
   channel holds another subscribe in progress, it waits for that one too
   (within the same timer), and decides only once the channel holds a committed
   subscription or nothing.


## Results

Every config of the last full run (`make tla SPEC=client CONFIGS=all`) gave the
expected result: each `MC_*` config passes, each `bug_*` config fails. Times are
of that run on a 10-core laptop (TLC 2.19); some ran next to other work, so take
them as rough. The bounds of each config are in `gen_configs.py`.

The default set of `make tla` takes about 7 minutes; `all` takes about
2.7 hours.

| Config | Distinct states | Time | Default set |
|---|---|---|---|
| `MC_connect` | 13359762 | 310s (5 min) |  |
| `MC_connection` | 3082656 | 82s | yes |
| `MC_connection_client_refresh` | 854048 | 22s | yes |
| `MC_connection_large` | 66159560 | 1970s (33 min) |  |
| `MC_double_invoke` | 1376963 | 26s | yes |
| `MC_large` | 33446667 | 759s (13 min) |  |
| `MC_liveness` | 41320 | 18s | yes |
| `MC_liveness_close` | 166028 | 54s |  |
| `MC_liveness_expiry` | 709969 | 151s (3 min) |  |
| `MC_not_positioned` | 10042600 | 351s (6 min) |  |
| `MC_presence_tick` | 5026975 | 148s (2 min) |  |
| `MC_recovery_epoch` | 894869 | 25s | yes |
| `MC_recovery_history_overflow` | 77210 | 5s | yes |
| `MC_recovery_lagging_replica` | 1712991 | 48s | yes |
| `MC_server` | 51823192 | 1389s (23 min) |  |
| `MC_small` | 6088936 | 120s (2 min) | yes |
| `MC_sub_expiry_client` | 24685638 | 615s (10 min) |  |
| `MC_sub_expiry_server` | 24212691 | 635s (11 min) |  |
| `MC_timeouts` | 93588403 | 2363s (39 min) |  |
| `MC_two_channels` | 24424724 | 613s (10 min) |  |

| Bug config | Time | Default set |
|---|---|---|
| `bug_async_unsubscribe_any_gen` | 1s | yes |
| `bug_cras_single_wait` | 82s |  |
| `bug_disconnect_push_not_last` | 1s | yes |
| `bug_empty_epoch_kept_after_merge` | 1s | yes |
| `bug_merge_no_gap_check` | 3s | yes |
| `bug_no_attempt_end_call` | 1s | yes |
| `bug_no_attempt_end_call_liveness` | 3s | yes |
| `bug_no_attempt_end_wait` | 1s | yes |
| `bug_no_connect_defer` | 1s | yes |
| `bug_no_join_gate` | 1s | yes |
| `bug_no_wait_other_after_subscribe` | 2s | yes |
| `bug_offsetless_before_result` | 1s | yes |
| `bug_pending_write_unlocked` | 2s | yes |
| `bug_pending_write_unlocked_duplicate` | 1s | yes |
| `bug_pending_write_unlocked_server` | 2s | yes |
| `bug_push_after_handler` | 6s | yes |
| `bug_push_after_handler_wire` | 32s | yes |
| `bug_server_sub_early_release` | 2s | yes |
| `bug_server_unsub_not_ordered` | 6s | yes |
| `bug_sub_refresh_no_gen_check` | 2s | yes |
| `bug_tick_remove_ignores_subscription` | 2s | yes |
| `bug_tick_remove_no_pending_leave` | 2s | yes |

`MC_timeouts` checks `AtMostOneUnsubscribeHandler`,
`NoUnsubscribeHandlerDuringConnectHandler`, `LeaveAfterJoin`,
`ReleasedAtQuiescence` and `CleanAfterClose`. The other properties fail only
through the documented exceptions of the 5 s and 30 s waits. Two of them are
worth knowing: when a resubscribe stops waiting for the previous subscription's
unsubscribe push, the push can follow the new subscription's reply and the SDK
ends the new subscription while the server keeps it; and with join/leave, the
old subscription's presence removal (by client id) can remove the new one's
presence.
