----------------------------- MODULE ClientBase -----------------------------
(***************************************************************************)
(* Centrifuge client subscription protocol (see README.md).               *)
(*                                                                         *)
(* One connection of one user.  Every goroutine of the Go code is a       *)
(* process in `pr` (id -> record with a pc); every action is named after  *)
(* the Go function it mirrors.  Steps are as coarse as the c.mu / hub     *)
(* shard lock / buffer lock critical sections of the Go code, and finer   *)
(* where another goroutine can interleave with the Go code.               *)
(*                                                                         *)
(* Actors:                                                                 *)
(*   SDK      - sends subscribe/unsubscribe commands, reads frames in     *)
(*              order (behaviour of centrifuge-js: an unsubscribe push    *)
(*              acts only on a subscribed subscription).                  *)
(*   reader   - <<"rd",0>>: connectCmd, ConnectHandler, then HandleCommand *)
(*              of the commands one at a time.                            *)
(*   callback - <<"cb",g>>: the application's SubscribeCallback of attempt *)
(*              g, invoked from an application goroutine at any time      *)
(*              (or never), possibly twice, then the rest of the          *)
(*              subscribe (subscribeCmd ...) on that goroutine.           *)
(*   server   - <<"su",i>> Client.Unsubscribe (Node.Unsubscribe),        *)
(*              <<"ss",i>> Client.Subscribe, <<"ins",i>> insufficient     *)
(*              state unsubscribes, <<"close",0>> close(),                 *)
(*              <<"ae",g>> UnsubscribeHandler call of an ended attempt.   *)
(*   broker   - publications with offsets (history) and without.          *)
(*   ghost    - application handler calls, wire monitor, join/leave.      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Chans,           \* channels
    ClientChans,     \* channels the SDK subscribes to (client-side subscriptions)
    ServerChans,     \* channels of server-side subscriptions (Client.Subscribe, connect)
    MaxCmds,         \* subscribe/unsubscribe commands the SDK may send
    MaxPubs,         \* publications with offset per channel
    MaxOffsetless,   \* publications without offset (in total)
    MaxServerUnsubs, \* Client.Unsubscribe calls by the application
    UnsubWhileSubscribing, \* restrict Client.Unsubscribe to channels with a subscribe in progress
    MaxServerSubs,   \* Client.Subscribe calls by the application
    MaxIns,          \* insufficient state unsubscribe goroutines
    ConnectSub,      \* ConnectReply.Subscriptions has a (positioned) channel
    Positioned,      \* subscriptions use positioning/recovery (stream path)
    Lossy,           \* broker PUB/SUB may lose a publication (history keeps it)
    JoinLeave,       \* subscriptions add presence and publish join/leave
    CloseAllowed,    \* the transport may close
    Timeouts,        \* bounded waits may give up (5s/30s), see README
    DoubleInvoke,    \* the application may invoke SubscribeCallback twice
    Answers,         \* subset of {"allow","deny","refuse","closedErr"}
    MaxDisconnects,  \* Client.Disconnect / Node.Disconnect calls by the application
    SlowWrite,       \* a write may find the queue over ClientQueueMaxSize (DisconnectSlow)
    StaleTimer,      \* ClientStaleCloseDelay: the stale timer of a new connection
    RefreshMode,     \* "none" (no expiry), "server" (RefreshHandler on expiry) or "client"
    MaxRefreshes,    \* refresh commands of the SDK (client mode) or Client.Refresh calls
    MaxExtends,      \* RefreshCallback answers which extend the connection
    MaxTicks,        \* presence ticks (updatePresence)
    SubExpiry,       \* client-side subscriptions expire: "none", "server" (SubRefreshHandler
                     \* from the presence tick) or "client" (sub_refresh commands)
    MaxSubRefreshes, \* SubRefreshHandler calls (tick) or sub_refresh commands
    MaxEpochs,       \* stream epochs of a channel (> 1: the stream may be reset)
    LaggingReplica,  \* a history read may return an empty stream (no epoch) from a lagging replica
    HistorySize,     \* publications kept in history (0: all), older positions are unrecoverable
    BufferLimit,     \* ClientQueueMaxSize as a number of publications held by a buffer (0: no limit)
    RefreshAnswers,  \* subset of {"extend","expired","error"}
    Bugs             \* set of old behaviours to reintroduce ({} = the current code)

AllBugs == {"PushAfterHandler", "ServerSubEarlyRelease", "NoAttemptEndCall",
            "NoJoinGate", "ServerUnsubNotOrdered", "NoConnectDefer", "NoAttemptEndWait",
            "PendingWriteUnlocked", "NoWaitOtherAfterSubscribe", "MergeNoGapCheck",
            "OffsetlessBeforeResult", "TickRemoveNoPendingLeave", "TickRemoveIgnoresSubscription",
            "SubRefreshNoGenCheck", "AsyncUnsubscribeAnyGen", "DisconnectPushNotLast",
            "EmptyEpochKeptAfterMerge", "CrasSingleWait"}
ASSUME Bugs \subseteq AllBugs
ASSUME Answers \subseteq {"allow", "deny", "refuse", "closedErr"}
ASSUME RefreshMode \in {"none", "server", "client"}
ASSUME SubExpiry \in {"none", "server", "client"}
ASSUME RefreshAnswers \subseteq {"extend", "expired", "error"}
(* Close reasons: the transport closed (no disconnect push), Client.Disconnect,  *)
(* slow queue, expired, stale, internal errors and insufficient state.         *)
CloseReasons == {"closed", "force", "slow", "expired", "stale", "error", "insufficient", "subexpired"}
(* writePendingPublication checks and writes a queued publication under c.mu,  *)
(* so the write is atomic with respect to an unsubscribe's removal, like the   *)
(* broadcast path is with respect to the hub removal. PendingWriteUnlocked:    *)
(* the old code wrote it after releasing c.mu.                                 *)
PendingWriteLocked == "PendingWriteUnlocked" \notin Bugs
(* A subscription without positioning or recovery queues its publications     *)
(* (StartQueueing) until its result is written. OffsetlessBeforeResult: the    *)
(* old code had no buffer for it.                                              *)
Queueing == "OffsetlessBeforeResult" \notin Bugs

MaxGen == MaxCmds + MaxServerSubs + (IF ConnectSub THEN 1 ELSE 0)
Gens == 1..MaxGen
ASSUME ClientChans \subseteq Chans /\ ServerChans \subseteq Chans /\ ClientChans \cap ServerChans = {}
ConnectChan == IF ServerChans # {} THEN CHOOSE c \in ServerChans : TRUE ELSE CHOOSE c \in Chans : TRUE
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Range(a, b) == a..b
Sorted(S) == [i \in 1..Cardinality(S) |-> CHOOSE x \in S : Cardinality({y \in S : y < x}) = i - 1]
Contiguous(S) == \A x \in S : x = Max(S) \/ x + 1 \in S
Last(s) == s[Len(s)]

AttemptEndOn == "NoAttemptEndCall" \notin Bugs
GateOn == JoinLeave /\ "NoJoinGate" \notin Bugs

VARIABLES
    srv,   \* server-side state of the connection (Client fields, hub, buffers)
    pr,    \* goroutines: id -> record
    brk,   \* broker and bounded environment counters
    net,   \* cmdQ (SDK -> server, FIFO), wire (server -> SDK, FIFO), open
    sdk,   \* SDK view
    mon,   \* wire monitor (ghost), updated on every frame written
    gh,    \* application handler calls and other ghosts
    viol,  \* violations recorded by actions (ghost)
    act    \* name of the last action, for traces only (not part of the VIEW)

vars == <<srv, pr, brk, net, sdk, mon, gh, viol, act>>
View == <<srv, pr, brk, net, sdk, mon, gh, viol>>

(* ChannelContext. gen = 0: no entry in c.channels. subCh: the entry has a  *)
(* non-nil subscribingCh (identified by gen, see closedSubCh).             *)
NoCtx == [gen |-> 0, subscribed |-> FALSE, serverSide |-> FALSE, allowed |-> FALSE,
          subCh |-> FALSE, pos |-> 0, ep |-> 0, pe |-> 0, exp |-> "none"]
          \* pos, ep: stream position; pe (ghost): the epoch pos really is of   \* exp: subscription expiry
Resv(g) == [NoCtx EXCEPT !.gen = g, !.subCh = TRUE]
(* Buffer (recovery.Buffer): pubs collected (phase 1) and queue (phase 2) hold   *)
(* Enc(epoch, offset); bep: epoch of the collected ones, mixed: of several       *)
(* epochs, povf/qovf: over the limit in phase 1/2.                               *)
NoBuf == [phase |-> "none", pubs |-> {}, queue |-> <<>>, bep |-> 0, mixed |-> FALSE,
          povf |-> FALSE, qovf |-> FALSE]
NewBuf(phase) == [NoBuf EXCEPT !.phase = phase]
Enc(e, o) == e * 10 + o
Off(x) == x % 10
Ep(x) == x \div 10
IsClose(p) == p[1] = "close"

(* unsubscribeWaiting frame *)
NoU == [ch |-> ConnectChan, code |-> "none", push |-> FALSE, disc |-> FALSE, sg |-> 0,
        tgt |-> 0, ended |-> FALSE, leaveNow |-> FALSE, rsub |-> FALSE,
        dk |-> "none", ret |-> "exit", dec |-> 0]  \* dec: the subscription an async unsubscribe was decided for
UWInit(c, code, push, disc, sg, ret) ==
    [NoU EXCEPT !.ch = c, !.code = code, !.push = push, !.disc = disc, !.sg = sg, !.ret = ret]
(* subscribeCmd frame *)
NoS == [g |-> 0, ch |-> ConnectChan, side |-> "none", id |-> 0, rec |-> FALSE, roff |-> 0,
        latest |-> 0, rpubs |-> {}, resOff |-> 0, pubsOut |-> <<>>, pos |-> 0, qw |-> 0,
        hadSubCh |-> FALSE, sbret |-> "exit", okret |-> "exit",
        rep |-> 0, lep |-> 0, recovered |-> FALSE, pe |-> 0]   \* requested/read epochs
SInit(g, c, side, id, rec, roff, okret) ==
    [NoS EXCEPT !.g = g, !.ch = c, !.side = side, !.id = id, !.rec = rec, !.roff = roff, !.okret = okret]

RD == <<"rd", 0>>
NoHolder == <<"none", 0>>  \* connectMu not held
RdInit == [pc |-> "connect", id |-> 0, ch |-> ConnectChan, rec |-> FALSE, roff |-> 0, rep |-> 0,
           exp |-> FALSE, pend |-> <<>>, lost |-> FALSE, closedDuring |-> FALSE, u |-> NoU, s |-> NoS]
CloseInit(reason) == [pc |-> "start", reason |-> reason, prev |-> "none", snap |-> {}, u |-> NoU]

Fr(t, c, id, g, off, pubs, code) ==
    [t |-> t, ch |-> c, id |-> id, gen |-> g, off |-> off, pubs |-> pubs, code |-> code, ep |-> 0, pe |-> 0]
(* A subscribe result with its stream position: epoch ep (0: empty), and the  *)
(* epoch the offset really belongs to (ghost pe).                            *)
FrRes(t, c, id, g, off, pubs, ep, pe) == [Fr(t, c, id, g, off, pubs, "none") EXCEPT !.ep = ep, !.pe = pe]
FrPub(c, g, o, e) == [Fr("pub", c, 0, g, o, <<>>, "none") EXCEPT !.ep = e]

-----------------------------------------------------------------------------
Init ==
    /\ srv = [status |-> "connecting",
              chans |-> [c \in Chans |-> NoCtx],
              closedSubCh |-> {},
              hub |-> [c \in Chans |-> 0],
              bufMap |-> [c \in Chans |-> 0],
              bufs |-> [g \in Gens |-> NoBuf],
              attemptEnds |-> [c \in Chans |-> 0],
              channelLeaves |-> [c \in Chans |-> 0],
              pendingUnsubs |-> 0,
              timedOut |-> FALSE,
              afterConnect |-> <<>>,
              genCounter |-> 0,
              handlersSet |-> FALSE,
              chRunning |-> FALSE,
              chStarted |-> FALSE,
              connectMu |-> NoHolder,
              closing |-> FALSE,
              authenticated |-> FALSE,
              cred |-> IF RefreshMode = "none" THEN "none" ELSE "valid",
              expTimer |-> FALSE,
              staleTimer |-> StaleTimer,
              presTimer |-> FALSE,
              presenceMu |-> NoHolder,
              presence |-> [c \in Chans |-> 0],
              gate |-> [g \in Gens |-> [joined |-> FALSE, left |-> FALSE]]]
    /\ pr = (RD :> RdInit)
    /\ brk = [hist |-> [c \in Chans |-> 0], bcast |-> [c \in Chans |-> 0], ol |-> 0,
              ep |-> [c \in Chans |-> 1],
              ins |-> 0, su |-> 0, ss |-> 0, closeReq |-> FALSE,
              disc |-> 0, slow |-> FALSE, refreshes |-> 0, extends |-> 0, rcbs |-> 0,
              ticks |-> 0, subRefreshes |-> 0, srcbs |-> 0, cras |-> 0]
    /\ net = [cmdQ |-> <<>>, wire |-> <<>>, open |-> TRUE]
    /\ sdk = [ch |-> [c \in Chans |-> [st |-> "unsub", pos |-> 0, ep |-> 0, hasPos |-> FALSE, id |-> 0,
                                       srv |-> FALSE, spos |-> 0]],
              up |-> FALSE, gone |-> FALSE, cmds |-> 0]
    /\ mon = [st |-> [c \in Chans |-> "idle"], gen |-> [c \in Chans |-> 0],
              pos |-> [c \in Chans |-> 0], lastRes |-> [c \in Chans |-> 0],
              ep |-> [c \in Chans |-> 0], pe |-> [c \in Chans |-> 0],
              bad |-> {}, replied |-> {}, discSent |-> FALSE]
    /\ gh = [subCalled |-> {}, allowed |-> {}, answered |-> {}, reinv |-> {},
             ends |-> [g \in Gens |-> 0], unsubDone |-> {},
             gch |-> [g \in Gens |-> ConnectChan], side |-> [g \in Gens |-> "none"],
             late |-> {}, closePhase |-> "open", disc |-> FALSE, discN |-> 0, closeReason |-> "none",
             joined |-> {}, left |-> {}, committed |-> {}]
    /\ viol = {}
    /\ act = <<"Init">>

-----------------------------------------------------------------------------
(* Process helpers *)
Step(p, r, sp) == pr' = [pr EXCEPT ![p] = r] @@ sp
Exit(p, sp) == pr' = [q \in DOMAIN pr \ {p} |-> pr[q]] @@ sp
UWRet(p, r, sp) == IF r.u.ret = "exit" THEN Exit(p, sp) ELSE Step(p, [r EXCEPT !.pc = r.u.ret], sp)
StartUW(r, c, code, push, disc, sg, ret) == [r EXCEPT !.pc = "uw_snap", !.u = UWInit(c, code, push, disc, sg, ret)]

(* go c.close(reason). A goroutine started on a closed client returns at once, *)
(* and one with the same reason as a close in progress changes nothing, so      *)
(* neither is spawned.                                                          *)
CloseSp(reason) ==
    IF <<"close", reason>> \in DOMAIN pr \/ srv.status = "closed" THEN <<>>
    ELSE (<<"close", reason>> :> CloseInit(reason))
(* endSubscribeAttempt: UnsubscribeHandler of an ended attempt on its own goroutine *)
AESp(g, c) == (<<"ae", g>> :> [pc |-> "run", g |-> g, ch |-> c])
(* handleInsufficientState: client-side -> handleAsyncUnsubscribe(ch, 0, insufficient),  *)
(* server-side -> close(DisconnectInsufficientState).                                    *)
InsSp(c, serverSide) ==
    IF serverSide THEN CloseSp("insufficient")
    ELSE IF brk.ins < MaxIns
         THEN (<<"ins", brk.ins + 1>> :> [pc |-> "uw_snap", u |-> UWInit(c, "insufficient", TRUE, FALSE, 0, "exit")])
         ELSE <<>>
InsCnt(c, serverSide) == IF ~serverSide /\ brk.ins < MaxIns THEN [brk EXCEPT !.ins = @ + 1] ELSE brk

(* Wire: frames written while the writer is open; the monitor checks them. *)
(* A publication (offset o, epoch e) on the wire. A result with an empty epoch  *)
(* adopts the epoch of the first publication; the offset it carries must then  *)
(* be of that epoch (pe), or publications of the new epoch go missing.         *)
PubStep(m, c, o, e) ==
    LET pos == Positioned /\ o > 0 /\ m.st[c] = "live" IN
    [m EXCEPT !.bad = @ \cup (IF m.st[c] # "live" THEN {"PubOutsideSubscription"} ELSE {})
                       \cup (IF pos /\ o # m.pos[c] + 1 THEN {"NonContiguousOffsets"} ELSE {})
                       \cup (IF pos /\ m.ep[c] # 0 /\ e # m.ep[c] THEN {"PublicationOfAnotherEpoch"} ELSE {})
                       \cup (IF pos /\ m.ep[c] = 0 /\ m.pe[c] # 0 /\ m.pe[c] # e
                             THEN {"EpochAdoptedOverAnotherEpoch"} ELSE {}),
              !.pos[c] = IF o > 0 THEN o ELSE @,
              !.ep[c] = IF pos THEN e ELSE @,
              !.pe[c] = IF pos THEN e ELSE @]
RECURSIVE PubsStep(_, _, _, _)
PubsStep(m, c, s, e) == IF s = <<>> THEN m ELSE PubsStep(PubStep(m, c, Head(s), e), c, Tail(s), e)
MonStep(m0, f) ==
    LET c == f.ch
        m == [m0 EXCEPT !.bad = @ \cup (IF m0.discSent THEN {"WriteAfterDisconnect"} ELSE {})]
    IN
    CASE f.t = "disconnect" ->
            [m EXCEPT !.discSent = TRUE,
                      !.bad = @ \cup (IF f.code # gh.closeReason THEN {"WrongDisconnectReason"} ELSE {})]
      [] f.t \in {"subOk", "subPush", "connSub"} ->
            PubsStep([m EXCEPT !.st[c] = "live", !.gen[c] = f.gen, !.pos[c] = f.off,
                               !.ep[c] = f.ep, !.pe[c] = f.pe,
                               !.lastRes[c] = IF f.gen > @ THEN f.gen ELSE @,
                               !.replied = @ \cup {f.id}], c, f.pubs, f.ep)
      [] f.t = "pub" -> PubStep(m, c, f.off, f.ep)
      [] f.t = "unsubReply" -> [m EXCEPT !.st[c] = "idle", !.replied = @ \cup {f.id}]
      [] f.t = "subErr" -> [m EXCEPT !.replied = @ \cup {f.id}]
      [] f.t = "unsubPush" ->
            [m EXCEPT !.bad = @ \cup (IF f.gen < m.lastRes[c] THEN {"PushAfterLaterResult"} ELSE {}),
                      !.st[c] = IF f.gen = m.gen[c] THEN "idle" ELSE @]
      [] OTHER -> m
RECURSIVE MonSeq(_, _)
MonSeq(m, fs) == IF fs = <<>> THEN m ELSE MonSeq(MonStep(m, Head(fs)), Tail(fs))
EmitN(n, fs) == /\ net' = IF n.open THEN [n EXCEPT !.wire = @ \o fs] ELSE n
                /\ mon' = IF n.open THEN MonSeq(mon, fs) ELSE mon
Emit(fs) == EmitN(net, fs)
NoEmit == UNCHANGED <<net, mon>>

(* c.mu-guarded tracking (subscribeTracking) *)
TotalAE(s) == LET F[S \in SUBSET Chans] == IF S = {} THEN 0 ELSE LET x == CHOOSE y \in S : TRUE IN s.attemptEnds[x] + F[S \ {x}]
              IN F[Chans]
AddAttemptEnd(s, c) == [s EXCEPT !.attemptEnds[c] = @ + 1, !.pendingUnsubs = @ + 1]       \* addAttemptEndLocked
AddPendingUnsub(s) == [s EXCEPT !.pendingUnsubs = @ + 1]                                   \* addPendingUnsubscribeLocked
AddPendingLeave(s, c) == [s EXCEPT !.channelLeaves[c] = @ + 1, !.pendingUnsubs = @ + 1]   \* addPendingLeaveLocked
FinishPendingLeave(s, c) == [s EXCEPT !.channelLeaves[c] = @ - 1, !.pendingUnsubs = @ - 1] \* finishPendingLeave
FinishAttemptEnd(s, c) ==                                                                   \* finishAttemptEnd
    LET s1 == [s EXCEPT !.attemptEnds[c] = @ - 1, !.pendingUnsubs = @ - 1]
    IN IF TotalAE(s1) = 0 THEN [s1 EXCEPT !.timedOut = FALSE] ELSE s1
HubRemove(s, c, g) == IF s.hub[c] = g THEN [s EXCEPT !.hub[c] = 0] ELSE s                  \* node.removeSubscription (gen-matched)
CancelBuf(s, c, g) ==                                                                       \* PubSubSync.CancelBuffering
    IF s.bufs[g].phase \in {"none", "stopped"} THEN s
    ELSE [s EXCEPT !.bufs[g] = NewBuf("stopped"),
                   !.bufMap[c] = IF @ = g THEN 0 ELSE @]
PresenceRemove(s, c) == IF JoinLeave THEN [s EXCEPT !.presence[c] = 0] ELSE s              \* removeSubscribePresence (by client id)

(* onSubscribeErrorGen: rollback of the reservation of generation g. *)
OSEGEnds(s, c, g) == s.chans[c].gen = g /\ s.chans[c].allowed /\ AttemptEndOn
OSEG(s, c, g) ==
    LET own == s.chans[c].gen = g
        s1 == IF own THEN [s EXCEPT !.chans[c] = NoCtx] ELSE s
        s2 == IF OSEGEnds(s, c, g) THEN AddAttemptEnd(s1, c) ELSE s1
        s3 == HubRemove(s2, c, g)
    IN IF own /\ s.chans[c].subCh THEN [s3 EXCEPT !.closedSubCh = @ \cup {g}] ELSE s3
GEnd(h, g, yes) == IF yes THEN [h EXCEPT !.ends[g] = @ + 1] ELSE h

RcbSp(kind) == (<<"rcb", brk.rcbs + 1>> :> [pc |-> "decide", kind |-> kind])
(* handleAsyncUnsubscribe(ch, subGen, unsubscribeExpired) of an expired subscription *)
ExpUnsubSp(c, g) ==
    IF <<"xu", g>> \in DOMAIN pr THEN <<>>
    ELSE (<<"xu", g>> :> [pc |-> "uw_snap",
                          u |-> [UWInit(c, "expired", TRUE, FALSE,
                                        IF "AsyncUnsubscribeAnyGen" \in Bugs THEN 0 ELSE g, "exit")
                                 EXCEPT !.dec = g]])

(* Ordering checks made at application handler calls *)
UnsubOrderViol(c, g) ==
    (IF \E g2 \in gh.subCalled : gh.gch[g2] = c /\ g2 > g THEN {"UnsubscribeAfterNextSubscribe"} ELSE {})

=============================================================================
