------------------------------- MODULE Client -------------------------------
(***************************************************************************)
(* Centrifuge client connection model (see README.md). The constants,      *)
(* variables and helpers are in ClientBase, the connection lifecycle in    *)
(* ClientConnection, presence ticks and subscription expiry in             *)
(* ClientPresence. This module: subscribe/unsubscribe (unsubscribeWaiting, *)
(* subscribeCmd, the reader, SubscribeCallbacks, Client.Subscribe), the    *)
(* broker, the SDK, the next-state relation and the properties.            *)
(***************************************************************************)
EXTENDS ClientConnection, ClientPresence

-----------------------------------------------------------------------------
(***************************************************************************)
(* unsubscribeWaiting(channel, unsub, disconnect, maxWait, push, subGen)  *)
(* (client.go).  Map/shared poll waits are not modelled.                  *)
(***************************************************************************)
UW_Snapshot(p) ==  \* c.mu.RLock(): read c.channels[channel]
    LET r == pr[p] u == r.u ctx == srv.chans[u.ch] IN
    /\ r.pc = "uw_snap"
    /\ IF u.sg # 0 /\ ctx.gen # u.sg THEN UWRet(p, r, <<>>)
       \* environment restriction of some configs: the application's Client.Unsubscribe
       \* only takes effect while a subscribe is in progress (else it is not called)
       ELSE IF UnsubWhileSubscribing /\ p[1] = "su" /\ ~(ctx.gen # 0 /\ ~ctx.subscribed) THEN UWRet(p, r, <<>>)
       ELSE IF ctx.gen = 0 THEN Step(p, [r EXCEPT !.pc = "uw_waitOther"], <<>>)
       ELSE IF ~ctx.serverSide /\ ~ctx.subscribed /\ ctx.subCh
            THEN Step(p, [r EXCEPT !.pc = "uw_waitSub", !.u.tgt = ctx.gen], <<>>)
            ELSE Step(p, [r EXCEPT !.pc = "uw_remove", !.u.tgt = ctx.gen], <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

UW_SubscribingChClosed(p) ==  \* case <-subscribingCh
    LET r == pr[p] IN
    /\ r.pc = "uw_waitSub" /\ r.u.tgt \in srv.closedSubCh
    /\ IF srv.chans[r.u.ch].gen = 0
       THEN IF "NoWaitOtherAfterSubscribe" \notin Bugs   \* channel gone: waitOtherUnsubscribe
            THEN Step(p, [r EXCEPT !.pc = "uw_waitOther"], <<>>)
            ELSE UWRet(p, r, <<>>)                       \* old: return nil
       ELSE Step(p, [r EXCEPT !.pc = "uw_remove"], <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

(* case <-tm.C: the wait for a subscribe in progress gave up (5s; for close() *)
(* min(5s, remaining of 10s)). With Timeouts at any time; without, only for   *)
(* close() waiting for an attempt whose SubscribeCallback was not invoked yet *)
(* (it may never be).                                                         *)
CallbackPending(g) == <<"cb", g>> \in DOMAIN pr /\ pr[<<"cb", g>>].pc = "decide"
UW_SubscribeWaitTimeout(p) ==
    LET r == pr[p] c == r.u.ch cur == srv.chans[c] IN
    /\ r.pc = "uw_waitSub" /\ r.u.tgt \notin srv.closedSubCh
    /\ Timeouts \/ (r.u.disc /\ CallbackPending(r.u.tgt))
    /\ srv' = IF cur.gen # 0 /\ cur.subCh
              THEN [srv EXCEPT !.chans[c].subCh = FALSE, !.closedSubCh = @ \cup {cur.gen}]
              ELSE srv
    /\ UWRet(p, r, CloseSp("error"))
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

UW_Remove(p) ==  \* c.mu.Lock(): identity-matched removal and tracking
    LET r == pr[p] u == r.u c == u.ch cur == srv.chans[c]
        removed == cur.gen # 0 /\ cur.gen = u.tgt
        ended == cur.allowed /\ ~cur.subscribed /\ AttemptEndOn
        pushAE == u.push /\ Bugs \cap {"ServerUnsubNotOrdered", "PushAfterHandler"} = {}
        aeTrack == ended \/ pushAE
        s1 == [srv EXCEPT !.chans[c] = NoCtx]
        s2 == IF cur.subCh THEN [s1 EXCEPT !.closedSubCh = @ \cup {cur.gen}] ELSE s1
        s3 == IF aeTrack THEN AddAttemptEnd(s2, c) ELSE AddPendingUnsub(s2)
        leaveNow == cur.subscribed /\ (IF GateOn THEN srv.gate[cur.gen].joined ELSE TRUE)
        s4 == IF cur.subscribed /\ JoinLeave THEN [s3 EXCEPT !.gate[cur.gen].left = TRUE] ELSE s3
        s5 == IF cur.subscribed /\ GateOn THEN AddPendingLeave(s4, c) ELSE s4
        s6 == IF u.push /\ "PushAfterHandler" \notin Bugs THEN AddPendingLeave(s5, c) ELSE s5
        otherSub == removed /\ u.dec # 0 /\ cur.gen # u.dec
    IN
    /\ r.pc = "uw_remove"
    /\ IF ~removed
       THEN /\ Step(p, [r EXCEPT !.pc = "uw_waitOther"], <<>>)
            /\ UNCHANGED srv
       ELSE /\ srv' = s6
            /\ Step(p, [r EXCEPT !.pc = "uw_after", !.u.ended = ended, !.u.leaveNow = leaveNow,
                                 !.u.rsub = cur.subscribed,
                                 !.u.dk = IF ended THEN "spawn" ELSE IF pushAE THEN "ae" ELSE "pu"], <<>>)
    /\ viol' = viol \cup (IF otherSub THEN {"AsyncUnsubscribeOfOtherSubscription"} ELSE {})
    /\ UNCHANGED <<brk, net, sdk, mon, gh>>

UW_EndAttemptAndLeave(p) ==  \* endSubscribeAttempt, removePresenceAndLeave (if leaveNow)
    LET r == pr[p] u == r.u c == u.ch g == u.tgt
        s1 == IF u.leaveNow THEN PresenceRemove(srv, c) ELSE srv
        s2 == IF u.leaveNow /\ GateOn THEN FinishPendingLeave(s1, c) ELSE s1
        spawn == u.dk = "spawn"
    IN
    /\ r.pc = "uw_after"
    /\ srv' = s2
    /\ Step(p, [r EXCEPT !.pc = "uw_hub"], IF spawn THEN AESp(g, c) ELSE <<>>)
    /\ gh' = LET h1 == GEnd(gh, g, spawn) IN
             IF u.leaveNow /\ JoinLeave THEN [h1 EXCEPT !.left = @ \cup {g}] ELSE h1
    /\ viol' = viol \cup (IF u.leaveNow /\ JoinLeave /\ g \notin gh.joined THEN {"LeaveBeforeJoin"} ELSE {})
    /\ UNCHANGED <<brk, net, sdk, mon>>

UW_RemoveFromHub(p) ==  \* node.removeSubscription(channel, c, removedSubGen) under the hub shard lock
    LET r == pr[p] IN
    /\ r.pc = "uw_hub"
    /\ srv' = HubRemove(srv, r.u.ch, r.u.tgt)
    /\ Step(p, [r EXCEPT !.pc = "uw_push"], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

UW_SendUnsubscribe(p) ==  \* if push: sendUnsubscribe; finishPendingLeave
    LET r == pr[p] u == r.u IN
    /\ r.pc = "uw_push"
    /\ IF u.push /\ "PushAfterHandler" \notin Bugs
       THEN /\ Emit(<<Fr("unsubPush", u.ch, 0, u.tgt, 0, <<>>, u.code)>>)
            /\ srv' = FinishPendingLeave(srv, u.ch)
       ELSE /\ NoEmit /\ UNCHANGED srv
    /\ Step(p, [r EXCEPT !.pc = "uw_handler"], <<>>)
    /\ UNCHANGED <<brk, sdk, gh, viol>>

UW_UnsubscribeHandler(p) ==  \* eventHub.unsubscribeHandler(Subscribed: true); deferred finish*
    LET r == pr[p] u == r.u c == u.ch g == u.tgt
        call == u.rsub /\ srv.handlersSet
        s1 == CASE u.dk = "ae" -> FinishAttemptEnd(srv, c)
                [] u.dk = "pu" -> [srv EXCEPT !.pendingUnsubs = @ - 1]
                [] OTHER -> srv
    IN
    /\ r.pc = "uw_handler"
    /\ srv' = s1
    /\ gh' = IF call THEN [gh EXCEPT !.ends[g] = @ + 1, !.unsubDone = @ \cup {g}] ELSE gh
    /\ viol' = viol \cup (IF call THEN UnsubOrderViol(c, g) \cup (IF gh.disc THEN {"HandlerAfterDisconnect"} ELSE {})
                          ELSE {})
                    \cup (IF u.rsub /\ ~srv.handlersSet /\ srv.chRunning THEN {"UnsubscribeDuringConnectHandler"} ELSE {})
    /\ IF u.push /\ "PushAfterHandler" \in Bugs   \* old: Client.Unsubscribe sent the push after unsubscribe() returned
       THEN Emit(<<Fr("unsubPush", c, 0, g, 0, <<>>, u.code)>>)
       ELSE NoEmit
    /\ UWRet(p, r, <<>>)
    /\ UNCHANGED <<brk, sdk>>

UW_WaitOtherUnsubscribe(p) ==  \* waitOtherUnsubscribe (client unsubscribe which removed nothing)
    LET r == pr[p] u == r.u IN
    /\ r.pc = "uw_waitOther"
    /\ \/ u.disc \/ u.code # "client" \/ "PushAfterHandler" \in Bugs
       \/ srv.channelLeaves[u.ch] = 0
    /\ UWRet(p, r, <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

UW_WaitOtherTimeout(p) ==
    LET r == pr[p] IN
    /\ Timeouts /\ r.pc = "uw_waitOther" /\ srv.channelLeaves[r.u.ch] > 0
    /\ UWRet(p, r, <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

UWActions(p) == UW_Snapshot(p) \/ UW_SubscribingChClosed(p) \/ UW_Remove(p) \/ UW_EndAttemptAndLeave(p)
                \/ UW_RemoveFromHub(p) \/ UW_SendUnsubscribe(p) \/ UW_UnsubscribeHandler(p)
                \/ UW_WaitOtherUnsubscribe(p)

-----------------------------------------------------------------------------
(***************************************************************************)
(* subscribeCmd (client.go), shared by the client subscribe (side          *)
(* "client"), Client.Subscribe ("server") and connect-time subscriptions   *)
(* ("connect").  Errors are reported to the caller, which rolls back.      *)
(***************************************************************************)
(* The caller's handling of a failed subscribeCmd. *)
SubscribeFailed(p, s, sBase, isDisconnect) ==
    LET c == s.ch g == s.g
        s1 == PresenceRemove(CancelBuf(sBase, c, g), c)   \* subscribeCmd's deferred presence removal
        ends == OSEGEnds(s1, c, g)
    IN
    /\ srv' = OSEG(s1, c, g)
    /\ gh' = GEnd(gh, g, ends)
    /\ CASE s.side = "client" ->     \* handleSubscribe: onSubscribeErrorGen + writeDisconnectOrErrorFlush
              /\ Exit(p, (IF ends THEN AESp(g, c) ELSE <<>>) @@ (IF isDisconnect THEN CloseSp("error") ELSE <<>>))
              /\ IF isDisconnect THEN NoEmit ELSE Emit(<<Fr("subErr", c, s.id, g, 0, <<>>, "none")>>)
         [] s.side = "server" ->     \* Client.Subscribe returns the error
              /\ Exit(p, <<>>) /\ NoEmit
         [] OTHER ->                 \* connectCmd: rollbackConnectServerSideSubs, connect fails
              /\ Step(p, [pr[p] EXCEPT !.pc = "dead"], CloseSp("error")) /\ NoEmit

SC_StartBufferingAndAddSubscription(p) ==
    \* StartBuffering; client-side: check the reservation and status; node.addSubscription;
    \* check again; node.addPresence.
    LET r == pr[p] s == r.s c == s.ch g == s.g
        fail == s.side = "client" /\ (srv.chans[c].gen = 0 \/ srv.status = "closed")
        \* StartBuffering (positioned) or StartQueueing (straight to the queueing phase)
        s1 == IF Positioned \/ Queueing
              THEN [srv EXCEPT !.bufMap[c] = g,
                               !.bufs[g] = NewBuf(IF Positioned THEN "collect" ELSE "queue")]
              ELSE srv
        s2 == [s1 EXCEPT !.hub[c] = g]
        s3 == IF JoinLeave THEN [s2 EXCEPT !.presence[c] = g] ELSE s2
    IN
    /\ r.pc = "sc_add"
    /\ IF fail
       THEN SubscribeFailed(p, s, srv, TRUE)
       ELSE /\ srv' = s3
            /\ Step(p, [r EXCEPT !.pc = IF Positioned THEN "sc_hist" ELSE "sc_reply"], <<>>)
            /\ UNCHANGED gh /\ NoEmit
    /\ UNCHANGED <<brk, sdk, viol>>

SC_ReadHistory(p) ==  \* node.recoverHistory / node.streamTop; isStreamRecovered
    \* A lagging replica may return an empty stream: offset 0, no epoch. Recovery
    \* needs the same epoch and the position still in history.
    LET r == pr[p] s == r.s c == s.ch IN
    /\ r.pc = "sc_hist"
    /\ \E lag \in (IF LaggingReplica THEN {FALSE, TRUE} ELSE {FALSE}) :
         LET latest == IF lag THEN 0 ELSE brk.hist[c]
             lep == IF lag THEN 0 ELSE brk.ep[c]
             recovered == /\ s.rec /\ ~lag /\ s.rep = lep /\ s.roff <= latest
                          /\ (HistorySize = 0 \/ latest - s.roff <= HistorySize)
         IN Step(p, [r EXCEPT !.pc = "sc_read", !.s.latest = latest, !.s.lep = lep,
                              !.s.recovered = recovered,
                              !.s.rpubs = IF recovered THEN Range(s.roff + 1, latest) ELSE {}], <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

SC_ReadBuffered(p) ==
    \* ReadBuffered(latest) + MergePublications; the client-side subscribe then writes its reply.
    LET r == pr[p] s == r.s c == s.ch g == s.g b == srv.bufs[g]
        \* ReadBuffered(epoch, offset): not over the limit, one epoch, the read's one
        \* (an empty read epoch: compared among themselves only)
        okRead == ~b.povf /\ (b.pubs = {} \/ (~b.mixed /\ (s.lep = 0 \/ b.bep = s.lep)))
        bp == {Off(x) : x \in {x \in b.pubs : Off(x) > s.latest}}
        all == s.rpubs \cup bp
        after == {o \in all : o > s.latest}
        okMerge == /\ bp = {} \/ Cardinality(all) <= 1 \/ Contiguous(all)
                   \* MergePublications(..., readEpoch, readOffset): the offsets after the
                   \* read position continue it without a gap, unless the read epoch is
                   \* empty (MergeNoGapCheck: old, unchecked)
                   /\ "MergeNoGapCheck" \notin Bugs /\ bp # {} /\ s.lep # 0 =>
                        after = {} \/ (Min(after) = s.latest + 1 /\ Contiguous(after))
        maxSeen == Max(all \cup {0})
        pos == Max({s.latest, maxSeen})
        \* the epoch the position really is of (ghost): of the buffered publications
        \* when they moved it past the read
        pe == IF maxSeen > s.latest /\ bp # {} THEN b.bep ELSE s.lep
        \* after the merge (its gap check skipped for an empty read epoch), an empty
        \* read epoch takes the epoch of the merged buffered publications
        \* (CollectedEpoch). EmptyEpochKeptAfterMerge: the old code kept it empty.
        lep == IF "EmptyEpochKeptAfterMerge" \notin Bugs /\ s.lep = 0 /\ bp # {} THEN b.bep ELSE s.lep
        resOff == IF s.recovered THEN s.roff ELSE pos
        out == IF s.recovered THEN Sorted(all) ELSE <<>>
        s1 == [srv EXCEPT !.bufs[g] = [NewBuf("queue") EXCEPT !.queue = b.queue, !.qovf = b.qovf]]
        r1 == [r EXCEPT !.s.pos = pos, !.s.resOff = resOff, !.s.pubsOut = out, !.s.pe = pe, !.s.lep = lep]
    IN
    /\ r.pc = "sc_read"
    /\ IF ~okRead \/ ~okMerge
       THEN SubscribeFailed(p, s, srv, TRUE)   \* DisconnectInsufficientState
       ELSE /\ srv' = s1
            /\ UNCHANGED gh
            /\ IF s.side = "client"
               THEN /\ Emit(<<FrRes("subOk", c, s.id, g, resOff, out, lep, pe)>>)
                    /\ Step(p, [r1 EXCEPT !.pc = "sc_commit"], <<>>)
               ELSE /\ NoEmit
                    /\ Step(p, [r1 EXCEPT !.pc = s.okret], <<>>)
    /\ UNCHANGED <<brk, sdk, viol>>

SC_WriteReply(p) ==  \* non-positioned: writeEncodedCommandReply (client) or return (server/connect)
    LET r == pr[p] s == r.s IN
    /\ r.pc = "sc_reply"
    /\ IF s.side = "client"
       THEN /\ Emit(<<FrRes("subOk", s.ch, s.id, s.g, 0, <<>>, 0, 0)>>)
            /\ Step(p, [r EXCEPT !.pc = "sc_commit"], <<>>)
       ELSE /\ NoEmit /\ Step(p, [r EXCEPT !.pc = s.okret], <<>>)
    /\ UNCHANGED <<srv, brk, sdk, gh, viol>>

(* commitSubscription(channel, ctx, reservationChannels), all three paths. *)
CommitCtx(s, cur, serverSide) ==
    [gen |-> s.g, subscribed |-> TRUE, serverSide |-> serverSide, allowed |-> cur.allowed,
     subCh |-> FALSE, pos |-> s.pos, ep |-> s.lep, pe |-> s.pe,
     exp |-> IF SubExpiry # "none" /\ s.side = "client" THEN "valid" ELSE "none"]

(* The deferred close(subscribingCh) after StopBuffering. *)
ReleaseSubCh(sv, c, g, had) == IF had THEN [sv EXCEPT !.closedSubCh = @ \cup {g}] ELSE sv

SC_CommitSubscription(p) ==  \* client-side commit, then StopBuffering
    LET r == pr[p] s == r.s c == s.ch g == s.g cur == srv.chans[c]
        lost == cur.gen # g
        closed == srv.status = "closed"
        endIt == ~lost /\ closed /\ cur.allowed /\ AttemptEndOn
        sl == PresenceRemove(CancelBuf(HubRemove(srv, c, g), c, g), c)
        sc0 == [srv EXCEPT !.chans[c] = NoCtx]
        sc1 == IF endIt THEN AddAttemptEnd(sc0, c) ELSE sc0
        sc2 == PresenceRemove(CancelBuf(HubRemove(sc1, c, g), c, g), c)
        sc3 == IF cur.subCh THEN [sc2 EXCEPT !.closedSubCh = @ \cup {g}] ELSE sc2
    IN
    /\ r.pc = "sc_commit"
    /\ CASE lost ->   \* reservation lost: roll back own hub entry, the caller disconnects
              /\ srv' = sl /\ Exit(p, CloseSp("error")) /\ UNCHANGED gh
         [] closed ->
              /\ srv' = sc3 /\ Exit(p, IF endIt THEN AESp(g, c) ELSE <<>>) /\ gh' = GEnd(gh, g, endIt)
         [] OTHER ->
              /\ srv' = [srv EXCEPT !.chans[c] = CommitCtx(s, cur, FALSE)]
              /\ Step(p, [r EXCEPT !.pc = "sb", !.s.hadSubCh = cur.subCh, !.s.sbret = "sc_release"], <<>>)
              /\ gh' = [gh EXCEPT !.committed = @ \cup {g}]
    /\ UNCHANGED <<brk, net, sdk, mon, viol>>

(* StopBuffering(buf, writePendingPublication): each queued publication is *)
(* checked and written under c.mu (writePendingPublication); with          *)
(* PendingWriteUnlocked written after c.mu is released: two steps.         *)
(* writePublicationUpdatePosition for a subscribed context: epoch (an empty one *)
(* adopts the publication's), then offset. "write", "drop" or "ins".          *)
LiveOutcome(ctx, o, e) ==
    CASE ~Positioned -> "write"
      [] ctx.ep # 0 /\ ctx.ep # e -> "ins"
      [] o > ctx.pos + 1 -> "ins"
      [] o < ctx.pos + 1 -> "drop"
      [] OTHER -> "write"
LiveCtx(ctx, o, e) == IF Positioned THEN [ctx EXCEPT !.pos = o, !.ep = e, !.pe = e] ELSE ctx
(* A publication dropped as already delivered must be of the position's epoch. *)
DropViol(ctx, o, e) ==
    IF Positioned /\ LiveOutcome(ctx, o, e) = "drop" /\ ctx.pe # 0 /\ ctx.pe # e
    THEN {"PublicationOfAnotherEpochDropped"} ELSE {}

SB_StopBuffering(p) ==
    LET r == pr[p] s == r.s c == s.ch g == s.g b == srv.bufs[g] ctx == srv.chans[c] IN
    /\ r.pc = "sb"
    /\ CASE b.phase = "none" ->   \* no buffer (OffsetlessBeforeResult)
              /\ Step(p, [r EXCEPT !.pc = s.sbret], <<>>) /\ UNCHANGED <<srv, brk, viol>> /\ NoEmit
         [] s.qw # 0 ->   \* writeEncodedPushData of the checked publication
              /\ Emit(<<FrPub(c, g, Off(s.qw), Ep(s.qw))>>)
              /\ Step(p, [r EXCEPT !.s.qw = 0], <<>>) /\ UNCHANGED <<srv, brk, viol>>
         [] b.queue # <<>> ->
              LET x == Head(b.queue) o == Off(x) e == Ep(x)
                  s1 == [srv EXCEPT !.bufs[g].queue = Tail(@)]
                  live == ctx.gen # 0 /\ ctx.subscribed
                  out == LiveOutcome(ctx, o, e)
              IN CASE ~live \/ out = "drop" ->
                        /\ srv' = s1 /\ Step(p, r, <<>>) /\ UNCHANGED brk /\ NoEmit
                        /\ viol' = viol \cup (IF live THEN DropViol(ctx, o, e) ELSE {})
                   [] out = "ins" ->
                        /\ srv' = s1 /\ Step(p, r, InsSp(c, ctx.serverSide)) /\ brk' = InsCnt(c, ctx.serverSide)
                        /\ NoEmit /\ UNCHANGED viol
                   [] PendingWriteLocked ->
                        /\ srv' = [s1 EXCEPT !.chans[c] = LiveCtx(ctx, o, e)]
                        /\ Emit(<<FrPub(c, g, o, e)>>)
                        /\ Step(p, r, <<>>) /\ UNCHANGED <<brk, viol>>
                   [] OTHER ->
                        /\ srv' = [s1 EXCEPT !.chans[c] = LiveCtx(ctx, o, e)]
                        /\ Step(p, [r EXCEPT !.s.qw = x], <<>>) /\ UNCHANGED <<brk, viol>> /\ NoEmit
         [] OTHER ->      \* queue empty: stopped, buffer removed; overflowed: insufficient state
              /\ srv' = [srv EXCEPT !.bufs[g] = NewBuf("stopped"),
                                    !.bufMap[c] = IF @ = g THEN 0 ELSE @]
              /\ IF b.qovf
                 THEN /\ Step(p, [r EXCEPT !.pc = s.sbret], InsSp(c, s.side # "client"))
                      /\ brk' = InsCnt(c, s.side # "client")
                 ELSE Step(p, [r EXCEPT !.pc = s.sbret], <<>>) /\ UNCHANGED brk
              /\ NoEmit /\ UNCHANGED viol
    /\ UNCHANGED <<sdk, gh>>

SC_CloseSubscribingCh(p) ==  \* subscribeCmd: deferred close(subscribingCh) on return
    LET r == pr[p] s == r.s IN
    /\ r.pc = "sc_release"
    /\ srv' = ReleaseSubCh(srv, s.ch, s.g, s.hadSubCh)
    /\ IF JoinLeave THEN Step(p, [r EXCEPT !.pc = "join"], <<>>) ELSE Exit(p, <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

(* publishJoinAndPresence + finishJoin (joinGate.join). Next pc: r.s.okret *)
(* for the reader, exit otherwise.                                          *)
JoinAndFinishJoin(p) ==
    LET r == pr[p] s == r.s c == s.ch g == s.g
        left == GateOn /\ srv.gate[g].left
        s1 == [srv EXCEPT !.gate[g].joined = TRUE]
        s2 == IF left THEN FinishPendingLeave(PresenceRemove(s1, c), c) ELSE s1
    IN
    /\ r.pc = "join"
    /\ srv' = s2
    /\ gh' = [gh EXCEPT !.joined = @ \cup {g}, !.left = IF left THEN @ \cup {g} ELSE @]
    /\ viol' = viol \cup (IF \E g2 \in gh.joined : gh.gch[g2] = c /\ g2 # g /\ g2 \notin gh.left
                          THEN {"JoinBeforePreviousLeave"} ELSE {})
                    \cup (IF g \in gh.left THEN {"LeaveBeforeJoin"} ELSE {})
    /\ IF p = RD THEN Step(p, [r EXCEPT !.pc = "rd_ch"], <<>>) ELSE Exit(p, <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon>>

SCActions(p) == SC_StartBufferingAndAddSubscription(p) \/ SC_ReadHistory(p) \/ SC_ReadBuffered(p)
                \/ SC_WriteReply(p) \/ SC_CommitSubscription(p) \/ SB_StopBuffering(p)
                \/ SC_CloseSubscribingCh(p) \/ JoinAndFinishJoin(p)

-----------------------------------------------------------------------------
(***************************************************************************)
(* Reader: connectCmd, triggerConnect/connect, HandleCommand loop.         *)
(***************************************************************************)
RD_ConnectCmd ==  \* connectCmd: reserve connect-time subscriptions, run their subscribeCmd
    LET r == pr[RD] g == srv.genCounter + 1 c == ConnectChan IN
    /\ RD \in DOMAIN pr /\ r.pc = "connect"
    /\ IF srv.status = "closed"
       THEN /\ Step(RD, [r EXCEPT !.pc = "dead"], <<>>) /\ UNCHANGED <<srv, gh>>
       ELSE IF ConnectSub
            THEN /\ srv' = [srv EXCEPT !.genCounter = g, !.chans[c] = Resv(g), !.authenticated = TRUE]
                 /\ gh' = [gh EXCEPT !.side[g] = "connect", !.gch[g] = c]
                 /\ Step(RD, [r EXCEPT !.pc = "sc_add", !.s = SInit(g, c, "connect", 0, FALSE, 0, "rd_connReply")], <<>>)
            ELSE /\ Step(RD, [r EXCEPT !.pc = "rd_connReply"], <<>>) /\ UNCHANGED gh
                 /\ srv' = [srv EXCEPT !.authenticated = TRUE]
    /\ UNCHANGED <<brk, net, sdk, mon, viol>>

RD_WriteConnectReply ==  \* writeEncodedCommandReply(connect) with the subscription results
    LET r == pr[RD] s == r.s IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_connReply"
    /\ Emit(<<Fr("connect", ConnectChan, 0, 0, 0, <<>>, "none")>> \o
            (IF ConnectSub THEN <<FrRes("connSub", s.ch, 0, s.g, s.resOff, s.pubsOut, s.lep, s.pe)>> ELSE <<>>))
    /\ Step(RD, [r EXCEPT !.pc = IF ConnectSub THEN "rd_connCommit" ELSE "rd_ch"], <<>>)
    /\ UNCHANGED <<srv, brk, sdk, gh, viol>>

RD_ConnectCommit ==  \* connectCmd: c.mu: take over the reservations (or drop them if closed)
    LET r == pr[RD] s == r.s c == s.ch g == s.g cur == srv.chans[c]
        lost == cur.gen # g
        closedDuring == srv.status = "closed"
    IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_connCommit"
    /\ srv' = CASE lost -> CancelBuf(srv, c, g)
                [] closedDuring -> CancelBuf([srv EXCEPT !.chans[c] = NoCtx], c, g)
                [] OTHER -> [srv EXCEPT !.chans[c] = CommitCtx(s, cur, TRUE)]
    /\ gh' = IF lost \/ closedDuring THEN gh ELSE [gh EXCEPT !.committed = @ \cup {g}]
    /\ Step(RD, [r EXCEPT !.lost = lost, !.closedDuring = closedDuring,
                          !.s.hadSubCh = ~lost /\ cur.subCh,
                          !.pc = IF lost \/ closedDuring THEN "rd_connRel" ELSE "sb",
                          !.s.sbret = "rd_connRel"], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, viol>>

RD_ConnectRelease ==  \* close(subscribingCh)s; roll back lost/closed ones
    LET r == pr[RD] s == r.s c == s.ch g == s.g
        s1 == IF r.lost \/ r.closedDuring THEN srv ELSE ReleaseSubCh(srv, c, g, s.hadSubCh)
        s1b == IF (r.lost \/ r.closedDuring) /\ s.hadSubCh THEN [srv EXCEPT !.closedSubCh = @ \cup {g}] ELSE s1
        s2 == IF r.lost \/ r.closedDuring THEN PresenceRemove(HubRemove(s1b, c, g), c) ELSE s1
    IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_connRel"
    /\ srv' = s2
    /\ Step(RD, [r EXCEPT !.pc = CASE r.closedDuring -> "dead"
                                   [] r.lost \/ ~JoinLeave -> "rd_ch"
                                   [] OTHER -> "join"], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

RD_ConnectHandlerStart ==  \* connect(): connectMu, ConnectHandler starts
    LET r == pr[RD] IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_ch" /\ srv.connectMu = NoHolder
    /\ IF srv.status # "connecting"
       THEN /\ Step(RD, [r EXCEPT !.pc = "dead"], <<>>) /\ UNCHANGED srv
       ELSE /\ srv' = [srv EXCEPT !.connectMu = RD, !.chRunning = TRUE, !.chStarted = TRUE]
            /\ Step(RD, [r EXCEPT !.pc = "rd_chEnd"], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

RD_ConnectHandlerEnd ==  \* ConnectHandler set OnUnsubscribe etc. and returned; statusConnected
    LET r == pr[RD] IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_chEnd"
    /\ srv' = [srv EXCEPT !.handlersSet = TRUE, !.status = "connected", !.chRunning = FALSE,
                          !.connectMu = NoHolder, !.afterConnect = <<>>]
    /\ Step(RD, [r EXCEPT !.pc = "rd_pend", !.pend = srv.afterConnect], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

RD_UnsubscribeAfterConnect ==  \* triggerConnect: c.Unsubscribe for the deferred calls
    LET r == pr[RD] IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_pend"
    /\ IF r.pend = <<>>
       THEN Step(RD, [r EXCEPT !.pc = "rd_timers"], <<>>)
       ELSE IF srv.status = "closed"
            THEN Step(RD, [r EXCEPT !.pend = Tail(r.pend)], <<>>)
            ELSE Step(RD, StartUW([r EXCEPT !.pend = Tail(r.pend)], Head(r.pend), "server", TRUE, FALSE, 0, "rd_pend"), <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

RD_ScheduleOnConnectTimers ==  \* scheduleOnConnectTimers: expire timer (stale timer stopped)
    LET r == pr[RD] IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_timers"
    /\ srv' = IF srv.status = "closed" THEN srv   \* scheduleNextTimer does nothing on a closed client
              ELSE [srv EXCEPT !.staleTimer = FALSE, !.expTimer = srv.cred # "none", !.presTimer = TRUE]
    /\ Step(RD, [r EXCEPT !.pc = "idle"], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

RD_HandleCommand ==  \* HandleCommand: next command from the connection
    LET r == pr[RD] cmd == Head(net.cmdQ) IN
    /\ RD \in DOMAIN pr /\ r.pc = "idle" /\ srv.status # "closed" /\ net.cmdQ # <<>>
    /\ net' = [net EXCEPT !.cmdQ = Tail(@)]
    /\ CASE cmd.t = "sub" ->
              Step(RD, [r EXCEPT !.pc = "rd_sub", !.id = cmd.id, !.ch = cmd.ch, !.rec = cmd.rec, !.rep = cmd.rep,
                                 !.roff = cmd.roff, !.exp = FALSE], <<>>)
         [] cmd.t = "refresh" ->
              Step(RD, [r EXCEPT !.pc = "rd_refresh"], <<>>)
         [] cmd.t = "subRefresh" ->
              Step(RD, [r EXCEPT !.pc = "rd_subRefresh", !.ch = cmd.ch], <<>>)
         [] OTHER ->
              Step(RD, StartUW([r EXCEPT !.id = cmd.id, !.ch = cmd.ch], cmd.ch, "client", FALSE, FALSE, 0, "rd_unsubReply"), <<>>)
    /\ UNCHANGED <<srv, brk, sdk, mon, gh, viol>>

RD_HandleRefresh ==  \* handleRefresh: RefreshHandler(event, cb), the callback on its own goroutine
    LET r == pr[RD] n == brk.rcbs + 1 IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_refresh"
    /\ brk' = [brk EXCEPT !.rcbs = n]
    /\ Step(RD, [r EXCEPT !.pc = "idle"], (<<"rcb", n>> :> [pc |-> "decide", kind |-> "client"]))
    /\ UNCHANGED <<srv, net, sdk, mon, gh, viol>>

RD_HandleSubRefresh ==  \* handleSubRefresh: subscribed context, SubRefreshHandler(event, cb)
    LET r == pr[RD] c == r.ch cur == srv.chans[c] n == brk.srcbs + 1 ok == cur.gen # 0 /\ cur.subscribed IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_subRefresh"
    /\ IF ok
       THEN /\ brk' = [brk EXCEPT !.srcbs = n]
            /\ Step(RD, [r EXCEPT !.pc = "idle"],
                     (<<"srcb", n>> :> [pc |-> "decide", kind |-> "cmd", ch |-> c, g |-> cur.gen]))
            /\ NoEmit
       ELSE /\ Emit(<<Fr("subRefreshReply", c, 0, 0, 0, <<>>, "denied")>>)   \* ErrorPermissionDenied
            /\ Step(RD, [r EXCEPT !.pc = "idle"], <<>>)
            /\ UNCHANGED brk
    /\ UNCHANGED <<srv, sdk, gh, viol>>

MustWaitAttemptEnds(c) ==  \* mustWaitAttemptEndsLocked (fewer than maxPendingAttemptEnds calls)
    \/ srv.channelLeaves[c] > 0
    \/ ("NoAttemptEndWait" \notin Bugs /\ srv.attemptEnds[c] > 0 /\ ~srv.timedOut)

RD_HandleSubscribe ==  \* validateSubscribeRequest (reserve) + SubscribeHandler call
    LET r == pr[RD] c == r.ch g == srv.genCounter + 1 IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_sub"
    /\ CASE srv.status = "closed" ->   \* DisconnectConnectionClosed
              /\ Step(RD, [r EXCEPT !.pc = "idle"], <<>>) /\ UNCHANGED <<srv, gh, viol>> /\ NoEmit
         [] srv.chans[c].gen # 0 ->    \* ErrorAlreadySubscribed
              /\ Emit(<<Fr("subErr", c, r.id, 0, 0, <<>>, "already")>>)
              /\ Step(RD, [r EXCEPT !.pc = "idle"], <<>>) /\ UNCHANGED <<srv, gh, viol>>
         [] MustWaitAttemptEnds(c) /\ ~r.exp ->
              /\ Step(RD, [r EXCEPT !.pc = "rd_subWait"], <<>>) /\ UNCHANGED <<srv, gh, viol>> /\ NoEmit
         [] OTHER ->
              /\ srv' = [srv EXCEPT !.genCounter = g, !.chans[c] = Resv(g)]
              /\ gh' = [gh EXCEPT !.subCalled = @ \cup {g}, !.gch[g] = c, !.side[g] = "client"]
              /\ viol' = viol \cup
                   (IF \E g2 \in gh.allowed : gh.gch[g2] = c /\ g2 \notin gh.unsubDone
                    THEN {"SubscribeBeforePreviousUnsubscribe"} ELSE {})
                   \cup (IF gh.disc THEN {"HandlerAfterDisconnect"} ELSE {})   \* SubscribeHandler
              /\ Step(RD, [r EXCEPT !.pc = "idle"],
                      (<<"cb", g>> :> [pc |-> "decide", g |-> g, ch |-> c, id |-> r.id, rec |-> r.rec, rep |-> r.rep,
                                        roff |-> r.roff, s |-> NoS]))
              /\ NoEmit
    /\ UNCHANGED <<brk, sdk>>

RD_WaitAttemptEnds ==  \* waitAttemptEnds: woken by signalLocked
    LET r == pr[RD] IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_subWait" /\ ~MustWaitAttemptEnds(r.ch)
    /\ Step(RD, [r EXCEPT !.pc = "rd_sub"], <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

RD_WaitAttemptEndsTimeout ==  \* 5s: attemptEndsTimedOut, SubscribeHandler is called anyway
    LET r == pr[RD] IN
    /\ Timeouts /\ RD \in DOMAIN pr /\ r.pc = "rd_subWait" /\ MustWaitAttemptEnds(r.ch)
    /\ srv' = [srv EXCEPT !.timedOut = TotalAE(srv) > 0]
    /\ Step(RD, [r EXCEPT !.pc = "rd_sub", !.exp = TRUE], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

RD_WriteUnsubscribeReply ==  \* handleUnsubscribe: reply after unsubscribe() returned
    LET r == pr[RD] IN
    /\ RD \in DOMAIN pr /\ r.pc = "rd_unsubReply"
    /\ Emit(<<Fr("unsubReply", r.ch, r.id, 0, 0, <<>>, "none")>>)
    /\ Step(RD, [r EXCEPT !.pc = "idle"], <<>>)
    /\ UNCHANGED <<srv, brk, sdk, gh, viol>>

RDActions == RD_ConnectCmd \/ RD_WriteConnectReply \/ RD_ConnectCommit \/ RD_ConnectRelease
             \/ RD_ConnectHandlerStart \/ RD_ConnectHandlerEnd \/ RD_UnsubscribeAfterConnect
             \/ RD_HandleCommand \/ RD_HandleSubscribe \/ RD_WaitAttemptEnds \/ RD_WriteUnsubscribeReply

-----------------------------------------------------------------------------
(***************************************************************************)
(* SubscribeCallback of attempt g (handleSubscribe's cb), invoked by the   *)
(* application at any time: answerSubscribeCallback, then the rollback or *)
(* subscribeCmd.  "refuse" = allowed, then refused by Centrifuge          *)
(* (expired); "closedErr" = DisconnectConnectionClosed error.             *)
(***************************************************************************)
CB_SubscribeCallback(p, k) ==
    LET r == pr[p] c == r.ch g == r.g
        closed == srv.status = "closed"
        allowed == k \in {"allow", "refuse"}
        resvOk == srv.chans[c].gen = g
        goneEnd == allowed /\ ~resvOk /\ AttemptEndOn          \* reservation already gone: end it here
        s1 == IF allowed /\ resvOk THEN [srv EXCEPT !.chans[c].allowed = TRUE] ELSE srv
        s2 == IF goneEnd THEN AddAttemptEnd(s1, c) ELSE s1
        rbEnds == OSEGEnds(s2, c, g)
        h1 == [gh EXCEPT !.answered = @ \cup {g},
                         !.allowed = IF allowed THEN @ \cup {g} ELSE @,
                         !.late = IF gh.closePhase = "done" THEN @ \cup {g} ELSE @]
        aeSp == IF goneEnd \/ rbEnds THEN AESp(g, c) ELSE <<>>
        rollback == closed \/ k # "allow"
    IN
    /\ p[1] = "cb" /\ r.pc = "decide"
    /\ IF rollback
       THEN /\ srv' = OSEG(s2, c, g)
            /\ gh' = GEnd(h1, g, goneEnd \/ rbEnds)
            /\ Exit(p, aeSp @@ (IF k = "closedErr" /\ ~closed THEN CloseSp("closed") ELSE <<>>))
            /\ IF closed \/ k = "closedErr" THEN NoEmit
               ELSE Emit(<<Fr("subErr", c, r.id, g, 0, <<>>, k)>>)
       ELSE /\ srv' = s2
            /\ gh' = GEnd(h1, g, goneEnd)
            /\ Step(p, [r EXCEPT !.pc = "sc_add", !.s = [SInit(g, c, "client", r.id, r.rec, r.roff, "exit") EXCEPT !.rep = r.rep]],
                    IF goneEnd THEN AESp(g, c) ELSE <<>>)
            /\ NoEmit
    /\ UNCHANGED <<brk, sdk, viol>>

(* A second invocation of the callback: answerSubscribeCallback's `answered` *)
(* guard logs an error and returns; nothing else happens.                    *)
CB_SecondInvocation(g) ==
    /\ DoubleInvoke /\ g \in gh.answered /\ g \notin gh.reinv
    /\ gh' = [gh EXCEPT !.reinv = @ \cup {g}]
    /\ UNCHANGED <<srv, pr, brk, net, sdk, mon, viol>>

AE_UnsubscribeHandler(p) ==  \* endSubscribeAttempt's goroutine: handler(Subscribed: false); finishAttemptEnd
    LET r == pr[p] c == r.ch g == r.g IN
    /\ p[1] = "ae" /\ r.pc = "run"
    /\ srv' = FinishAttemptEnd(srv, c)
    /\ gh' = [gh EXCEPT !.unsubDone = @ \cup {g}]
    /\ viol' = viol \cup UnsubOrderViol(c, g)
                    \cup (IF gh.disc /\ g \notin gh.late THEN {"HandlerAfterDisconnect"} ELSE {})
    /\ Exit(p, <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon>>

-----------------------------------------------------------------------------
(* Client.Subscribe (server-side subscription) *)
SS_Reserve(p) ==
    LET r == pr[p] c == r.ch g == srv.genCounter + 1 IN
    /\ p[1] = "ss" /\ r.pc = "ss_res"
    /\ srv.channelLeaves[c] = 0 \/ r.exp \/ srv.status = "closed"   \* mustWaitPendingLeaveLocked
    /\ IF srv.status = "closed" \/ srv.chans[c].gen # 0
       THEN /\ Exit(p, <<>>) /\ UNCHANGED <<srv, gh>>
       ELSE /\ srv' = [srv EXCEPT !.genCounter = g, !.chans[c] = Resv(g)]
            /\ gh' = [gh EXCEPT !.gch[g] = c, !.side[g] = "server"]
            /\ Step(p, [r EXCEPT !.pc = "sc_add", !.s = SInit(g, c, "server", 0, FALSE, 0, "ss_push")], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, viol>>

SS_WaitPendingLeaveTimeout(p) ==
    /\ Timeouts /\ p[1] = "ss" /\ pr[p].pc = "ss_res" /\ ~pr[p].exp
    /\ Step(p, [pr[p] EXCEPT !.exp = TRUE], <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

SS_WriteSubscribePush(p) ==  \* writeSubscribePush, before the commit
    LET r == pr[p] s == r.s IN
    /\ r.pc = "ss_push"
    /\ Emit(<<FrRes("subPush", s.ch, 0, s.g, s.resOff, s.pubsOut, s.lep, s.pe)>>)
    /\ Step(p, [r EXCEPT !.pc = "ss_commit"], <<>>)
    /\ UNCHANGED <<srv, brk, sdk, gh, viol>>

SS_CommitSubscription(p) ==
    LET r == pr[p] s == r.s c == s.ch g == s.g cur == srv.chans[c]
        lost == cur.gen # g
        closed == srv.status = "closed"
        sl == PresenceRemove(CancelBuf(HubRemove(srv, c, g), c, g), c)
        sc0 == PresenceRemove(CancelBuf(HubRemove([srv EXCEPT !.chans[c] = NoCtx], c, g), c, g), c)
        sc1 == IF cur.subCh THEN [sc0 EXCEPT !.closedSubCh = @ \cup {g}] ELSE sc0
        early == "ServerSubEarlyRelease" \in Bugs /\ cur.subCh
        sok == [srv EXCEPT !.chans[c] = CommitCtx(s, cur, TRUE),
                           !.closedSubCh = IF early THEN @ \cup {g} ELSE @]
    IN
    /\ r.pc = "ss_commit"
    /\ CASE lost -> /\ srv' = sl /\ Exit(p, <<>>) /\ UNCHANGED gh
         [] closed -> /\ srv' = sc1 /\ Exit(p, <<>>) /\ UNCHANGED gh
         [] OTHER ->
              /\ srv' = sok
              /\ gh' = [gh EXCEPT !.committed = @ \cup {g}]
              /\ Step(p, [r EXCEPT !.pc = "sb", !.s.hadSubCh = cur.subCh /\ ~early, !.s.sbret = "sc_release"], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, viol>>

SU_Unsubscribe(p) ==  \* Client.Unsubscribe
    LET r == pr[p] IN
    /\ p[1] = "su" /\ r.pc = "su_start"
    /\ CASE srv.status = "closed" -> Exit(p, <<>>) /\ UNCHANGED srv
         [] srv.status = "connecting" /\ "NoConnectDefer" \notin Bugs ->   \* unsubscribesAfterConnect
              /\ srv' = [srv EXCEPT !.afterConnect = Append(@, r.ch)] /\ Exit(p, <<>>)
         [] OTHER -> Step(p, StartUW(r, r.ch, "server", TRUE, FALSE, 0, "exit"), <<>>) /\ UNCHANGED srv
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

-----------------------------------------------------------------------------
(* Broker and hub *)
(* writePublication -> SyncPublication / writeOffsetlessPublication /       *)
(* writePublicationUpdatePosition, under the hub shard read lock (so it is *)
(* atomic with respect to node.removeSubscription). b1: broker state after. *)
(* SyncPublication: collected (phase 1) or queued (phase 2) up to BufferLimit; *)
(* over it the buffer overflows and drops what it holds.                       *)
Collect(b, o, e) ==
    CASE b.povf -> b
      [] BufferLimit > 0 /\ Cardinality(b.pubs) >= BufferLimit -> [b EXCEPT !.povf = TRUE, !.pubs = {}]
      [] OTHER -> [b EXCEPT !.pubs = @ \cup {Enc(e, o)}, !.bep = IF b.pubs = {} THEN e ELSE @,
                            !.mixed = @ \/ (b.pubs # {} /\ e # b.bep)]
Queue(b, o, e) ==
    CASE b.qovf -> b
      [] BufferLimit > 0 /\ Len(b.queue) >= BufferLimit -> [b EXCEPT !.qovf = TRUE, !.queue = <<>>]
      [] OTHER -> [b EXCEPT !.queue = Append(@, Enc(e, o))]

WritePublication(c, o, e, b1) ==
    LET bg == srv.bufMap[c] ctx == srv.chans[c]
        buffering == bg # 0 /\ srv.bufs[bg].phase \in {"collect", "queue"}
        out == LiveOutcome(ctx, o, e)
    IN
    CASE srv.hub[c] = 0 -> UNCHANGED <<srv, pr>> /\ brk' = b1 /\ NoEmit
      [] buffering ->   \* collected for ReadBuffered, or queued for StopBuffering; no offset: dropped
           /\ srv' = IF o = 0 THEN srv
                     ELSE IF srv.bufs[bg].phase = "collect"
                          THEN [srv EXCEPT !.bufs[bg] = Collect(@, o, e)]
                          ELSE [srv EXCEPT !.bufs[bg] = Queue(@, o, e)]
           /\ UNCHANGED pr /\ brk' = b1 /\ NoEmit
      [] o = 0 ->       \* writeOffsetlessPublication: no channel context check
           Emit(<<FrPub(c, srv.hub[c], 0, 0)>>) /\ UNCHANGED <<srv, pr>> /\ brk' = b1
      [] ctx.gen = 0 \/ ~ctx.subscribed -> UNCHANGED <<srv, pr>> /\ brk' = b1 /\ NoEmit
      [] out = "drop" -> UNCHANGED <<srv, pr>> /\ brk' = b1 /\ NoEmit /\ viol' = viol \cup DropViol(ctx, o, e)
      [] out = "ins" ->   \* insufficient state (epoch or offset)
           /\ pr' = pr @@ InsSp(c, ctx.serverSide)
           /\ brk' = IF ~ctx.serverSide /\ brk.ins < MaxIns THEN [b1 EXCEPT !.ins = @ + 1] ELSE b1
           /\ UNCHANGED srv /\ NoEmit
      [] OTHER ->
           /\ srv' = [srv EXCEPT !.chans[c] = LiveCtx(ctx, o, e)]
           /\ Emit(<<FrPub(c, ctx.gen, o, e)>>) /\ UNCHANGED pr /\ brk' = b1

Publish(c) ==  \* node.Publish with history: offset assigned
    /\ brk.hist[c] < MaxPubs
    /\ brk' = [brk EXCEPT !.hist[c] = @ + 1]
    /\ UNCHANGED <<srv, pr, net, sdk, mon, gh, viol>>

BroadcastStep(c) ==  \* PUB/SUB delivery (in offset order) -> hub.broadcastPublication
    /\ brk.bcast[c] < brk.hist[c]
    /\ WritePublication(c, brk.bcast[c] + 1, brk.ep[c], [brk EXCEPT !.bcast[c] = @ + 1])
    /\ IF LiveOutcome(srv.chans[c], brk.bcast[c] + 1, brk.ep[c]) = "drop" /\ srv.hub[c] # 0
          /\ ~(srv.bufMap[c] # 0 /\ srv.bufs[srv.bufMap[c]].phase \in {"collect", "queue"})
          /\ srv.chans[c].gen # 0 /\ srv.chans[c].subscribed
       THEN TRUE ELSE UNCHANGED viol
    /\ UNCHANGED <<sdk, gh>>

EpochReset(c) ==  \* the stream is reset (removed, expired): a new epoch, offsets from 1
    /\ brk.ep[c] < MaxEpochs /\ brk.bcast[c] = brk.hist[c]
    /\ brk' = [brk EXCEPT !.ep[c] = @ + 1, !.hist[c] = 0, !.bcast[c] = 0]
    /\ UNCHANGED <<srv, pr, net, sdk, mon, gh, viol>>

LosePublication(c) ==  \* PUB/SUB loses it (history keeps it)
    /\ Lossy /\ brk.bcast[c] < brk.hist[c]
    /\ brk' = [brk EXCEPT !.bcast[c] = @ + 1]
    /\ UNCHANGED <<srv, pr, net, sdk, mon, gh, viol>>

PublishOffsetless(c) ==  \* publication without offset, broadcast right away
    /\ brk.ol < MaxOffsetless
    /\ WritePublication(c, 0, 0, [brk EXCEPT !.ol = @ + 1])
    /\ UNCHANGED <<sdk, gh, viol>>

ServerUnsubscribe(c) ==  \* application calls Client.Unsubscribe / Node.Unsubscribe
    /\ brk.su < MaxServerUnsubs /\ srv.status # "closed"
    /\ UnsubWhileSubscribing => srv.chans[c].gen # 0 /\ ~srv.chans[c].subscribed
    /\ brk' = [brk EXCEPT !.su = @ + 1]
    /\ pr' = pr @@ (<<"su", brk.su + 1>> :> [pc |-> "su_start", ch |-> c, u |-> NoU])
    /\ UNCHANGED <<srv, net, sdk, mon, gh, viol>>

ServerSubscribe(c) ==  \* application calls Client.Subscribe on a connected client
    /\ c \in ServerChans /\ brk.ss < MaxServerSubs /\ srv.status = "connected"
    /\ brk' = [brk EXCEPT !.ss = @ + 1]
    /\ pr' = pr @@ (<<"ss", brk.ss + 1>> :> [pc |-> "ss_res", ch |-> c, exp |-> FALSE, s |-> NoS])
    /\ UNCHANGED <<srv, net, sdk, mon, gh, viol>>

TransportClose ==  \* the transport closed: ClientCloseFunc -> close(DisconnectConnectionClosed)
    /\ CloseAllowed /\ ~brk.closeReq /\ srv.status # "closed"
    /\ brk' = [brk EXCEPT !.closeReq = TRUE]
    /\ pr' = pr @@ CloseSp("closed")
    /\ UNCHANGED <<srv, net, sdk, mon, gh, viol>>

-----------------------------------------------------------------------------
(* SDK (centrifuge-js behaviour) *)
SDK_Subscribe(c) ==
    LET v == sdk.ch[c] id == sdk.cmds + 1 IN
    /\ c \in ClientChans /\ sdk.up /\ ~sdk.gone /\ sdk.cmds < MaxCmds /\ v.st = "unsub" /\ ~v.srv
    /\ net' = [net EXCEPT !.cmdQ = Append(@, [t |-> "sub", ch |-> c, id |-> id,
                                              rec |-> Positioned /\ v.hasPos, roff |-> v.pos, rep |-> v.ep])]
    /\ sdk' = [sdk EXCEPT !.cmds = id, !.ch[c].st = "subscribing", !.ch[c].id = id]
    /\ UNCHANGED <<srv, pr, brk, mon, gh, viol>>

SDK_Unsubscribe(c) ==
    LET id == sdk.cmds + 1 IN
    /\ sdk.up /\ ~sdk.gone /\ sdk.cmds < MaxCmds /\ sdk.ch[c].st \in {"subscribing", "subscribed"}
    /\ net' = [net EXCEPT !.cmdQ = Append(@, [t |-> "unsub", ch |-> c, id |-> id, rec |-> FALSE, roff |-> 0, rep |-> 0])]
    /\ sdk' = [sdk EXCEPT !.cmds = id, !.ch[c].st = "unsub"]
    /\ UNCHANGED <<srv, pr, brk, mon, gh, viol>>

LastOr(s, d) == IF s = <<>> THEN d ELSE Last(s)

SDK_Read ==
    LET f == Head(net.wire) c == f.ch v == sdk.ch[c] n1 == [net EXCEPT !.wire = Tail(@)] IN
    /\ net.wire # <<>> /\ ~sdk.gone
    /\ CASE f.t = "connect" -> sdk' = [sdk EXCEPT !.up = TRUE] /\ net' = n1
         [] f.t \in {"connSub", "subPush"} ->
              sdk' = [sdk EXCEPT !.ch[c].srv = TRUE, !.ch[c].spos = LastOr(f.pubs, f.off)] /\ net' = n1
         [] f.t = "subOk" ->
              /\ sdk' = IF v.st = "subscribing" /\ v.id = f.id
                        THEN [sdk EXCEPT !.ch[c].st = "subscribed", !.ch[c].pos = LastOr(f.pubs, f.off),
                                         !.ch[c].ep = f.ep, !.ch[c].hasPos = Positioned]
                        ELSE sdk
              /\ net' = n1
         [] f.t = "subErr" ->
              /\ sdk' = IF v.st = "subscribing" /\ v.id = f.id THEN [sdk EXCEPT !.ch[c].st = "unsub"] ELSE sdk
              /\ net' = n1
         [] f.t = "pub" ->
              /\ sdk' = CASE v.st = "subscribed" -> [sdk EXCEPT !.ch[c].pos = IF f.off > 0 THEN f.off ELSE @,
                                                                 !.ch[c].ep = IF f.off > 0 THEN f.ep ELSE @]
                          [] v.srv -> [sdk EXCEPT !.ch[c].spos = IF f.off > 0 THEN f.off ELSE @]
                          [] OTHER -> sdk
              /\ net' = n1
         [] f.t = "unsubPush" ->
              IF v.st = "subscribed"
              THEN IF f.code = "insufficient" /\ sdk.cmds < MaxCmds   \* resubscribe with recovery
                   THEN /\ sdk' = [sdk EXCEPT !.cmds = @ + 1, !.ch[c].st = "subscribing", !.ch[c].id = sdk.cmds + 1]
                        /\ net' = [n1 EXCEPT !.cmdQ = Append(@, [t |-> "sub", ch |-> c, id |-> sdk.cmds + 1,
                                                                rec |-> Positioned, roff |-> v.pos, rep |-> v.ep])]
                   ELSE sdk' = [sdk EXCEPT !.ch[c].st = "unsub"] /\ net' = n1
              ELSE IF v.st = "unsub" /\ v.srv
                   THEN sdk' = [sdk EXCEPT !.ch[c].srv = FALSE] /\ net' = n1
                   ELSE sdk' = sdk /\ net' = n1
         [] OTHER -> sdk' = sdk /\ net' = n1   \* unsubscribe reply
    /\ UNCHANGED <<srv, pr, brk, mon, gh, viol>>

-----------------------------------------------------------------------------
(* Steps of the library goroutine p (act names the step in traces). *)
Internal(p) ==
    /\ p \in DOMAIN pr
    /\ \/ UW_Snapshot(p) /\ act' = <<"UW_Snapshot", p>>
       \/ UW_SubscribingChClosed(p) /\ act' = <<"UW_SubscribingChClosed", p>>
       \/ UW_Remove(p) /\ act' = <<"UW_Remove", p>>
       \/ UW_EndAttemptAndLeave(p) /\ act' = <<"UW_EndAttemptAndLeave", p>>
       \/ UW_RemoveFromHub(p) /\ act' = <<"UW_RemoveFromHub", p>>
       \/ UW_SendUnsubscribe(p) /\ act' = <<"UW_SendUnsubscribe", p>>
       \/ UW_UnsubscribeHandler(p) /\ act' = <<"UW_UnsubscribeHandler", p>>
       \/ UW_WaitOtherUnsubscribe(p) /\ act' = <<"UW_WaitOtherUnsubscribe", p>>
       \/ SC_StartBufferingAndAddSubscription(p) /\ act' = <<"SC_StartBufferingAndAddSubscription", p>>
       \/ SC_ReadHistory(p) /\ act' = <<"SC_ReadHistory", p>>
       \/ SC_ReadBuffered(p) /\ act' = <<"SC_ReadBuffered", p>>
       \/ SC_WriteReply(p) /\ act' = <<"SC_WriteReply", p>>
       \/ SC_CommitSubscription(p) /\ act' = <<"SC_CommitSubscription", p>>
       \/ SB_StopBuffering(p) /\ act' = <<"SB_StopBuffering", p>>
       \/ SC_CloseSubscribingCh(p) /\ act' = <<"SC_CloseSubscribingCh", p>>
       \/ JoinAndFinishJoin(p) /\ act' = <<"JoinAndFinishJoin", p>>
       \/ SS_Reserve(p) /\ act' = <<"SS_Reserve", p>>
       \/ SS_WriteSubscribePush(p) /\ act' = <<"SS_WriteSubscribePush", p>>
       \/ SS_CommitSubscription(p) /\ act' = <<"SS_CommitSubscription", p>>
       \/ SU_Unsubscribe(p) /\ act' = <<"SU_Unsubscribe", p>>
       \/ AE_UnsubscribeHandler(p) /\ act' = <<"AE_UnsubscribeHandler", p>>
       \/ (p = RD /\ RD_ConnectCmd /\ act' = <<"RD_ConnectCmd", p>>)
       \/ (p = RD /\ RD_WriteConnectReply /\ act' = <<"RD_WriteConnectReply", p>>)
       \/ (p = RD /\ RD_ConnectCommit /\ act' = <<"RD_ConnectCommit", p>>)
       \/ (p = RD /\ RD_ConnectRelease /\ act' = <<"RD_ConnectRelease", p>>)
       \/ (p = RD /\ RD_ConnectHandlerStart /\ act' = <<"RD_ConnectHandlerStart", p>>)
       \/ (p = RD /\ RD_ConnectHandlerEnd /\ act' = <<"RD_ConnectHandlerEnd", p>>)
       \/ (p = RD /\ RD_UnsubscribeAfterConnect /\ act' = <<"RD_UnsubscribeAfterConnect", p>>)
       \/ (p = RD /\ RD_HandleCommand /\ act' = <<"RD_HandleCommand", p>>)
       \/ (p = RD /\ RD_HandleSubscribe /\ act' = <<"RD_HandleSubscribe", p>>)
       \/ (p = RD /\ RD_WaitAttemptEnds /\ act' = <<"RD_WaitAttemptEnds", p>>)
       \/ (p = RD /\ RD_WriteUnsubscribeReply /\ act' = <<"RD_WriteUnsubscribeReply", p>>)
       \/ (p = RD /\ RD_ScheduleOnConnectTimers /\ act' = <<"RD_ScheduleOnConnectTimers", p>>)
       \/ (p = RD /\ RD_HandleRefresh /\ act' = <<"RD_HandleRefresh", p>>)
       \/ CL_StartClosing(p) /\ act' = <<"CL_StartClosing", p>>
       \/ CL_SetClosed(p) /\ act' = <<"CL_SetClosed", p>>
       \/ CL_WriteDisconnectPush(p) /\ act' = <<"CL_WriteDisconnectPush", p>>
       \/ CL_CloseWriter(p) /\ act' = <<"CL_CloseWriter", p>>
       \/ CL_UnsubscribeLoop(p) /\ act' = <<"CL_UnsubscribeLoop", p>>
       \/ CL_DisconnectHandler(p) /\ act' = <<"CL_DisconnectHandler", p>>
       \/ EXP_Expire(p) /\ act' = <<"EXP_Expire", p>>
       \/ EXP_RefreshHandler(p) /\ act' = <<"EXP_RefreshHandler", p>>
       \/ RCB_WriteRefreshReply(p) /\ act' = <<"RCB_WriteRefreshReply", p>>
       \/ CHK_CheckExpired(p) /\ act' = <<"CHK_CheckExpired", p>>
       \/ CHK_RearmTimer(p) /\ act' = <<"CHK_RearmTimer", p>>
       \/ AREF_Update(p) /\ act' = <<"AREF_Update", p>>
       \/ AREF_WriteRefreshPush(p) /\ act' = <<"AREF_WriteRefreshPush", p>>
       \/ STALE_CloseStale(p) /\ act' = <<"STALE_CloseStale", p>>
       \/ CL_PresenceLock(p) /\ act' = <<"CL_PresenceLock", p>>
       \/ (p = RD /\ RD_HandleSubRefresh /\ act' = <<"RD_HandleSubRefresh", p>>)
       \/ T_Snapshot(p) /\ act' = <<"T_Snapshot", p>>
       \/ \E c \in Chans : T_UpdateChannelPresence(p, c) /\ act' = <<"T_UpdateChannelPresence", p, c>>
       \/ \E c \in Chans : T_AddPresence(p, c) /\ act' = <<"T_AddPresence", p, c>>
       \/ \E c \in Chans : T_CheckSubscriptionExpiration(p, c) /\ act' = <<"T_CheckSubscriptionExpiration", p, c>>
       \/ T_CompensateRacedPresence(p) /\ act' = <<"T_CompensateRacedPresence", p>>
       \/ T_RemoveRacedPresence(p) /\ act' = <<"T_RemoveRacedPresence", p>>
       \/ CRAS_Decide(p) /\ act' = <<"CRAS_Decide", p>>
       \/ CRAS_Remove(p) /\ act' = <<"CRAS_Remove", p>>
       \/ SRCB_WriteReply(p) /\ act' = <<"SRCB_WriteReply", p>>

(* Bounded waits giving up (timers). *)
GiveUp(p) ==
    /\ p \in DOMAIN pr
    /\ \/ UW_SubscribeWaitTimeout(p) /\ act' = <<"UW_SubscribeWaitTimeout", p>>
       \/ UW_WaitOtherTimeout(p) /\ act' = <<"UW_WaitOtherTimeout", p>>
       \/ SS_WaitPendingLeaveTimeout(p) /\ act' = <<"SS_WaitPendingLeaveTimeout", p>>
       \/ (p = RD /\ RD_WaitAttemptEndsTimeout /\ act' = <<"RD_WaitAttemptEndsTimeout", p>>)

(* The application invokes the SubscribeCallback of attempt p. *)
Answer(p) ==
    /\ p \in DOMAIN pr
    /\ \/ \E k \in Answers : CB_SubscribeCallback(p, k) /\ act' = <<"CB_SubscribeCallback", p, k>>
       \/ \E k \in RefreshAnswers : RCB_RefreshCallback(p, k) /\ act' = <<"RCB_RefreshCallback", p, k>>
       \/ \E k \in {"extend", "expired", "error"} : SRCB_SubRefreshCallback(p, k) /\ act' = <<"SRCB_SubRefreshCallback", p, k>>

(* Timers firing. *)
Timers ==
    \/ ExpireTimerFires /\ act' = <<"ExpireTimerFires">>
    \/ StaleTimerFires /\ act' = <<"StaleTimerFires">>
    \/ TickFires /\ act' = <<"TickFires">>

Broadcast(c) == BroadcastStep(c) /\ act' = <<"BroadcastPublication", c, brk.bcast[c] + 1>>
Read == SDK_Read /\ act' = <<"SDK_Read", Head(net.wire).t, Head(net.wire).ch>>

Environment ==
    \/ \E c \in Chans :
        \/ Publish(c) /\ act' = <<"Publish", c>>
        \/ LosePublication(c) /\ act' = <<"LosePublication", c>>
        \/ PublishOffsetless(c) /\ act' = <<"PublishOffsetless", c>>
        \/ ServerUnsubscribe(c) /\ act' = <<"ServerUnsubscribe", c>>
        \/ ServerSubscribe(c) /\ act' = <<"ServerSubscribe", c>>
        \/ SDK_Subscribe(c) /\ act' = <<"SDK_Subscribe", c>>
        \/ SDK_Unsubscribe(c) /\ act' = <<"SDK_Unsubscribe", c>>
        \/ SubTimeExpires(c) /\ act' = <<"SubTimeExpires", c>>
        \/ EpochReset(c) /\ act' = <<"EpochReset", c>>
        \/ SDK_SubRefresh(c) /\ act' = <<"SDK_SubRefresh", c>>
    \/ TransportClose /\ act' = <<"TransportClose">>
    \/ ServerDisconnect /\ act' = <<"ServerDisconnect">>
    \/ SlowWriteDetected /\ act' = <<"SlowWriteDetected">>
    \/ TimeExpires /\ act' = <<"TimeExpires">>
    \/ SDK_Refresh /\ act' = <<"SDK_Refresh">>
    \/ \E k \in {"extend", "expired", "none"} : ServerRefresh(k) /\ act' = <<"ServerRefresh", k>>
    \/ \E g \in Gens : CB_SecondInvocation(g) /\ act' = <<"CB_SecondInvocation", g>>

(* Nothing left to do: the reader is idle (or dead after close), every other  *)
(* goroutine is done, the connection is drained.  Any other state without a   *)
(* successor is a deadlock (a goroutine blocked forever), reported by TLC.    *)
AllDone ==
    /\ DOMAIN pr = {RD} /\ pr[RD].pc \in {"idle", "dead"}
    /\ net.wire = <<>> \/ sdk.gone
    /\ net.cmdQ = <<>> \/ srv.status = "closed"
Done == AllDone /\ UNCHANGED View /\ act' = <<"Done">>

Next ==
    \/ \E p \in DOMAIN pr : Internal(p) \/ GiveUp(p) \/ Answer(p)
    \/ \E c \in Chans : Broadcast(c)
    \/ Read
    \/ Timers
    \/ Environment
    \/ Done

RcbIds == {<<"rcb", i>> : i \in 1..(MaxRefreshes + MaxExtends + 1)}
SrcbIds == {<<"srcb", i>> : i \in 1..(MaxSubRefreshes + MaxTicks * Cardinality(Chans))}
PIds == {RD, <<"exp", 0>>, <<"stale", 0>>} \cup {<<"close", x>> : x \in CloseReasons}
        \cup RcbIds \cup {<<"aref", i>> : i \in 1..MaxRefreshes} \cup SrcbIds
        \cup {<<"tick", 0>>} \cup {<<"cras", c>> : c \in Chans} \cup {<<"xu", g>> : g \in Gens} \cup {<<"cb", g>> : g \in Gens} \cup {<<"ae", g>> : g \in Gens}
        \cup {<<"su", i>> : i \in 1..MaxServerUnsubs} \cup {<<"ss", i>> : i \in 1..MaxServerSubs}
        \cup {<<"ins", i>> : i \in 1..MaxIns}

(* Goroutines of the library run (weak fairness); the SDK reads its        *)
(* connection; the broker delivers.  The application (callbacks, calls),  *)
(* the SDK user, publishers and timers are not fair.                       *)
Fairness ==
    /\ \A p \in PIds : WF_vars(Internal(p))
    /\ WF_vars(Read)
    /\ \A c \in Chans : WF_vars(Broadcast(c))
    /\ WF_vars(ExpireTimerFires /\ act' = <<"ExpireTimerFires">>)

Spec == Init /\ [][Next]_vars /\ Fairness
(* The application eventually invokes every SubscribeCallback. *)
SpecAnswering == Spec /\ \A p \in {<<"cb", g>> : g \in Gens} \cup RcbIds \cup SrcbIds : WF_vars(Answer(p))

-----------------------------------------------------------------------------
(* Properties *)
TypeOK ==
    /\ srv.status \in {"connecting", "connected", "closed"}
    /\ srv.pendingUnsubs \in Nat
    /\ \A c \in Chans : srv.attemptEnds[c] \in Nat /\ srv.channelLeaves[c] \in Nat

Live(g) == srv.chans[gh.gch[g]].gen = g /\ srv.chans[gh.gch[g]].subscribed

(* Never more than one UnsubscribeHandler call per SubscribeHandler attempt *)
(* or server-side subscription.                                            *)
AtMostOneUnsubscribeHandler == \A g \in Gens : gh.ends[g] <= 1

(* Ordering of application handler calls. *)
UnsubscribeBeforeNextSubscribe ==
    /\ "UnsubscribeAfterNextSubscribe" \notin viol
    /\ "SubscribeBeforePreviousUnsubscribe" \notin viol
NothingAfterDisconnectHandler ==
    /\ "HandlerAfterDisconnect" \notin viol

NoUnsubscribeHandlerDuringConnectHandler == "UnsubscribeDuringConnectHandler" \notin viol
LeaveAfterJoin == "LeaveBeforeJoin" \notin viol
JoinAfterPreviousLeave == "JoinBeforePreviousLeave" \notin viol

(* A subscription refresh or an expiry unsubscribe acts only on the        *)
(* subscription it was decided for (subGen identity).                      *)
(* Publications are dropped as already delivered only within one epoch. *)
NoPublicationOfAnotherEpochDropped == "PublicationOfAnotherEpochDropped" \notin viol

NoRefreshOrExpiryOfOtherSubscription ==
    /\ "SubRefreshAppliedToOtherSubscription" \notin viol
    /\ "AsyncUnsubscribeOfOtherSubscription" \notin viol

(* Connection *)
DisconnectFrameOfWinner == "WrongDisconnectReason" \notin mon.bad
NothingAfterDisconnectFrame == "WriteAfterDisconnect" \notin mon.bad
NoExpireTimerOnClosedClient == srv.expTimer => srv.status # "closed"

(* Wire, per channel *)
NoPublicationOutsideSubscription == "PubOutsideSubscription" \notin mon.bad
ContiguousOffsets == "NonContiguousOffsets" \notin mon.bad
NoUnsubscribePushAfterLaterResult == "PushAfterLaterResult" \notin mon.bad

Quiescent ==
    /\ DOMAIN pr = {RD} /\ pr[RD].pc = "idle"
    /\ net.cmdQ = <<>> /\ net.wire = <<>> /\ net.open /\ sdk.up
    /\ \A c \in Chans : brk.bcast[c] = brk.hist[c]

ServerLive(c) == srv.chans[c].gen # 0 /\ srv.chans[c].subscribed
SdkPos(c) == IF sdk.ch[c].st = "subscribed" THEN sdk.ch[c].pos ELSE sdk.ch[c].spos

(* At quiescence the SDK view equals the server state and the hub, nothing *)
(* is left behind, and every ended attempt or subscription got its call.   *)
ConsistentAtQuiescence ==
    Quiescent =>
      /\ \A c \in Chans :
           /\ (sdk.ch[c].st = "subscribed" \/ sdk.ch[c].srv) = ServerLive(c)
           /\ sdk.ch[c].st # "subscribing"
           /\ ServerLive(c) => /\ srv.hub[c] = srv.chans[c].gen
                               /\ (sdk.ch[c].st = "subscribed") = ~srv.chans[c].serverSide
                               /\ (Positioned => SdkPos(c) = srv.chans[c].pos)
           /\ ~ServerLive(c) => srv.hub[c] = 0 /\ srv.chans[c].gen = 0
           /\ srv.bufMap[c] = 0
           /\ srv.attemptEnds[c] = 0 /\ srv.channelLeaves[c] = 0
           /\ JoinLeave => ((srv.presence[c] # 0) = ServerLive(c))
      /\ srv.pendingUnsubs = 0
      /\ \A g \in gh.allowed : Live(g) \/ (gh.ends[g] = 1 /\ g \in gh.unsubDone)
      /\ \A g \in gh.committed : srv.chStarted /\ ~Live(g) => gh.ends[g] = 1
      /\ JoinLeave => \A g \in gh.joined : (g \in gh.left) = ~Live(g)

(* The handler-call and tracking part of ConsistentAtQuiescence, which also  *)
(* holds with Timeouts.                                                        *)
ReleasedAtQuiescence ==
    Quiescent =>
      /\ srv.pendingUnsubs = 0
      /\ \A c \in Chans : srv.attemptEnds[c] = 0 /\ srv.channelLeaves[c] = 0 /\ srv.bufMap[c] = 0
                          /\ (ServerLive(c) => srv.hub[c] = srv.chans[c].gen)
      /\ \A g \in gh.allowed : Live(g) \/ (gh.ends[g] = 1 /\ g \in gh.unsubDone)
      /\ \A g \in gh.committed : srv.chStarted /\ ~Live(g) => gh.ends[g] = 1

(* After close, once every goroutine is done (callbacks never invoked keep *)
(* theirs): every allowed attempt got exactly one call, nothing is left in *)
(* the hub, buffers or presence.                                           *)
Finished == srv.status = "closed" /\ DOMAIN pr = {RD} /\ pr[RD].pc \in {"idle", "dead"}
CleanAfterClose ==
    Finished =>
      /\ \A c \in Chans : srv.hub[c] = 0 /\ srv.bufMap[c] = 0 /\ (JoinLeave => srv.presence[c] = 0)
                          /\ srv.chans[c].gen = 0
      /\ \A g \in gh.allowed : gh.ends[g] = 1 /\ g \in gh.unsubDone
      /\ srv.pendingUnsubs = 0
      /\ JoinLeave => gh.joined \subseteq gh.left

(* DisconnectHandler is called at most once, and exactly once when           *)
(* ConnectHandler was called, once everything is done.                       *)
DisconnectHandlerOnce ==
    /\ gh.discN <= 1
    /\ Finished => gh.discN = IF srv.chStarted THEN 1 ELSE 0

(* Liveness *)
(* A connection whose credentials stay expired is eventually closed. *)
ExpiredConnectionClosed == <>[](srv.cred = "expired") => <>(srv.status = "closed")
AllowedAttemptReleased ==
    \A g \in Gens : [](g \in gh.allowed => <>(g \in gh.unsubDone \/ Live(g)))
SubscribeCommandReplied ==
    \A id \in 1..MaxCmds :
      [](id \in {cmd.id : cmd \in {net.cmdQ[i] : i \in DOMAIN net.cmdQ}} =>
           <>(id \in mon.replied \/ ~net.open))
PublicationDelivered ==
    \A c \in Chans, o \in 1..MaxPubs :
      [](brk.hist[c] >= o /\ ServerLive(c) /\ sdk.ch[c].st = "subscribed"
           => <>(SdkPos(c) >= o \/ ~ServerLive(c) \/ sdk.ch[c].st # "subscribed" \/ ~net.open))
=============================================================================
