--------------------------- MODULE ClientPresence ---------------------------
(***************************************************************************)
(* Presence ticks and subscription expiry of Client.tla: updatePresence,   *)
(* compensateRacedPresence, checkSubscriptionExpiration, SubRefreshHandler *)
(* and sub_refresh.                                                         *)
(***************************************************************************)
EXTENDS ClientBase

-----------------------------------------------------------------------------
(***************************************************************************)
(* Presence tick (updatePresence) and subscription expiry.                 *)
(***************************************************************************)
TickFires ==  \* onTimerOp(timerOpPresence); presenceInFlight: one tick at a time
    /\ srv.presTimer /\ srv.status # "closed" /\ brk.ticks < MaxTicks
    /\ <<"tick", 0>> \notin DOMAIN pr
    /\ brk' = [brk EXCEPT !.ticks = @ + 1]
    /\ pr' = pr @@ (<<"tick", 0>> :> [pc |-> "start", snap |-> [c \in Chans |-> NoCtx], todoP |-> {},
                                       adding |-> {}, added |-> {}, todoE |-> {}, raced |-> {}])
    /\ UNCHANGED <<srv, net, sdk, mon, gh, viol>>

T_Snapshot(p) ==  \* presenceMu.Lock(); c.mu: status, snapshot of the subscribed channels with duties
    LET r == pr[p]
        S == {c \in Chans : srv.chans[c].subscribed /\ (JoinLeave \/ srv.chans[c].exp # "none")}
    IN
    /\ p[1] = "tick" /\ r.pc = "start" /\ srv.presenceMu = NoHolder
    /\ IF srv.status = "closed"
       THEN Exit(p, <<>>) /\ UNCHANGED srv
       ELSE /\ srv' = [srv EXCEPT !.presenceMu = p]
            /\ Step(p, [r EXCEPT !.pc = "work", !.snap = [c \in Chans |-> IF c \in S THEN srv.chans[c] ELSE NoCtx],
                                 !.todoP = IF JoinLeave THEN S ELSE {},
                                 !.todoE = {c \in S : srv.chans[c].exp # "none"}], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

T_UpdateChannelPresence(p, c) ==  \* updateChannelPresence: c.channels check under RLock, then the add
    LET r == pr[p] IN
    /\ p[1] = "tick" /\ r.pc = "work" /\ c \in r.todoP
    /\ Step(p, [r EXCEPT !.todoP = IF srv.closing THEN {} ELSE @ \ {c},   \* closing: stop
                         !.adding = IF ~srv.closing /\ srv.chans[c].gen # 0 THEN @ \cup {c} ELSE @], <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

T_AddPresence(p, c) ==  \* node.addPresence (a network call, after the check)
    LET r == pr[p] IN
    /\ p[1] = "tick" /\ r.pc = "work" /\ c \in r.adding
    /\ srv' = [srv EXCEPT !.presence[c] = r.snap[c].gen]
    /\ Step(p, [r EXCEPT !.adding = @ \ {c}, !.added = @ \cup {c}], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

T_CheckSubscriptionExpiration(p, c) ==
    \* checkSubscriptionExpiration of the snapshot entry: client-side refresh: unsubscribe
    \* expired (decided for this subscription); server-side: SubRefreshHandler(cb)
    LET r == pr[p] item == r.snap[c] n == brk.srcbs + 1 IN
    /\ p[1] = "tick" /\ r.pc = "work" /\ r.todoP = {} /\ r.adding = {} /\ c \in r.todoE
    /\ IF srv.closing \/ item.exp # "expired"
       THEN /\ Step(p, [r EXCEPT !.todoE = IF srv.closing THEN {} ELSE @ \ {c}], <<>>)
            /\ UNCHANGED <<brk, viol>>
       ELSE IF SubExpiry = "client"
            THEN /\ Step(p, [r EXCEPT !.todoE = @ \ {c}], ExpUnsubSp(c, item.gen))
                 /\ UNCHANGED <<brk, viol>>
            ELSE /\ Step(p, [r EXCEPT !.todoE = @ \ {c}],
                          (<<"srcb", n>> :> [pc |-> "decide", kind |-> "tick", ch |-> c, g |-> item.gen]))
                 /\ brk' = [brk EXCEPT !.srcbs = n]
                 /\ UNCHANGED viol
    /\ UNCHANGED <<srv, net, sdk, mon, gh>>

TickRemoves(cur, item) ==  \* tickPresenceToRemove (all modelled subscriptions emit presence)
    /\ cur.gen # item.gen
    /\ ("TickRemoveIgnoresSubscription" \in Bugs \/ ~(cur.gen # 0 /\ cur.subscribed))
TickWaitsSubscribe(cur, item) ==  \* a resubscribe in progress: decide once it is done
    /\ "TickRemoveIgnoresSubscription" \notin Bugs
    /\ cur.gen # 0 /\ cur.gen # item.gen /\ ~cur.subscribed /\ cur.subCh
PendingLeaveOn == "TickRemoveNoPendingLeave" \notin Bugs

T_CompensateRacedPresence(p) ==  \* compensateRacedPresence (deferred): c.mu, per added entry
    LET r == pr[p]
        wait == {c \in r.added : TickWaitsSubscribe(srv.chans[c], r.snap[c])}
        raced == {c \in r.added \ wait : TickRemoves(srv.chans[c], r.snap[c])}
        s1 == IF PendingLeaveOn
              THEN [srv EXCEPT !.channelLeaves = [c \in Chans |-> @[c] + (IF c \in raced THEN 1 ELSE 0)],
                               !.pendingUnsubs = @ + Cardinality(raced)]
              ELSE srv
        sp == [x \in {<<"cras", c>> : c \in wait} |->
                 [pc |-> "wait", ch |-> x[2], g |-> r.snap[x[2]].gen, w |-> srv.chans[x[2]].gen]]
    IN
    /\ p[1] = "tick" /\ r.pc = "work" /\ r.todoP = {} /\ r.adding = {} /\ r.todoE = {}
    /\ srv' = s1
    /\ pr' = [pr EXCEPT ![p] = [r EXCEPT !.pc = "remove", !.raced = raced]] @@ sp
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

T_RemoveRacedPresence(p) ==  \* removeRacedPresence + finishPendingLeave; presenceMu released
    LET r == pr[p]
        s1 == [srv EXCEPT !.presence = [c \in Chans |-> IF c \in r.raced THEN 0 ELSE @[c]],
                          !.presenceMu = NoHolder]
    IN
    /\ p[1] = "tick" /\ r.pc = "remove"
    /\ srv' = IF PendingLeaveOn
              THEN [s1 EXCEPT !.channelLeaves = [c \in Chans |-> @[c] - (IF c \in r.raced THEN 1 ELSE 0)],
                              !.pendingUnsubs = @ - Cardinality(r.raced)]
              ELSE s1
    /\ Exit(p, <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

CRAS_Decide(p) ==  \* compensateRacedPresenceAfterSubscribe: after <-subscribingCh (or 5s)
    LET r == pr[p] cur == srv.chans[r.ch] item == [NoCtx EXCEPT !.gen = r.g]
        rm == TickRemoves(cur, item)
        \* another subscribe in progress by now: wait for it too (within the same
        \* timer). CrasSingleWait: the old code decided after the first wait.
        rewait == "CrasSingleWait" \notin Bugs /\ TickWaitsSubscribe(cur, item) /\ cur.gen # r.w
    IN
    /\ p[1] = "cras" /\ r.pc = "wait" /\ (r.w \in srv.closedSubCh \/ Timeouts)
    /\ IF rewait THEN Step(p, [r EXCEPT !.w = cur.gen], <<>>) /\ UNCHANGED srv
       ELSE IF rm
       THEN /\ srv' = IF PendingLeaveOn THEN AddPendingLeave(srv, r.ch) ELSE srv
            /\ Step(p, [r EXCEPT !.pc = "remove"], <<>>)
       ELSE Exit(p, <<>>) /\ UNCHANGED srv
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

CRAS_Remove(p) ==
    LET r == pr[p] s1 == [srv EXCEPT !.presence[r.ch] = 0] IN
    /\ p[1] = "cras" /\ r.pc = "remove"
    /\ srv' = IF PendingLeaveOn THEN FinishPendingLeave(s1, r.ch) ELSE s1
    /\ Exit(p, <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

(* The application's SubRefreshCallback: of the tick (server-side refresh) or *)
(* of a sub_refresh command (client-side refresh).                           *)
SRCB_SubRefreshCallback(p, k) ==
    LET r == pr[p] c == r.ch cur == srv.chans[c]
        sameSub == cur.gen = r.g /\ cur.subscribed
        apply == IF "SubRefreshNoGenCheck" \in Bugs THEN cur.gen # 0 /\ cur.subscribed ELSE sameSub
    IN
    /\ p[1] = "srcb" /\ r.pc = "decide"
    /\ CASE k = "extend" ->   \* c.mu: gen-matched write-back of expireAt
              /\ srv' = IF apply THEN [srv EXCEPT !.chans[c].exp = "valid"] ELSE srv
              /\ viol' = viol \cup (IF apply /\ cur.gen # r.g THEN {"SubRefreshAppliedToOtherSubscription"} ELSE {})
              /\ IF r.kind = "cmd" THEN Step(p, [r EXCEPT !.pc = "reply"], <<>>) ELSE Exit(p, <<>>)
         [] r.kind = "tick" ->     \* resultCB(false): unsubscribe expired, this subscription
              /\ Exit(p, ExpUnsubSp(c, r.g)) /\ UNCHANGED <<srv, viol>>
         [] k = "expired" ->       \* sub_refresh: DisconnectSubExpired
              /\ Exit(p, CloseSp("subexpired")) /\ UNCHANGED <<srv, viol>>
         [] OTHER -> /\ Step(p, [r EXCEPT !.pc = "reply"], <<>>) /\ UNCHANGED <<srv, viol>>  \* error reply
    /\ UNCHANGED <<brk, net, sdk, mon, gh>>

SRCB_WriteReply(p) ==
    LET r == pr[p] IN
    /\ p[1] = "srcb" /\ r.pc = "reply"
    /\ Emit(<<Fr("subRefreshReply", r.ch, 0, r.g, 0, <<>>, "none")>>)
    /\ Exit(p, <<>>)
    /\ UNCHANGED <<srv, brk, sdk, gh, viol>>

SubTimeExpires(c) ==  \* time passes the subscription's expireAt
    /\ srv.chans[c].subscribed /\ srv.chans[c].exp = "valid"
    /\ srv' = [srv EXCEPT !.chans[c].exp = "expired"]
    /\ UNCHANGED <<pr, brk, net, sdk, mon, gh, viol>>

SDK_SubRefresh(c) ==  \* client-side subscription refresh: sub_refresh with a new token
    /\ SubExpiry = "client" /\ sdk.up /\ ~sdk.gone /\ sdk.ch[c].st = "subscribed"
    /\ brk.subRefreshes < MaxSubRefreshes
    /\ brk' = [brk EXCEPT !.subRefreshes = @ + 1]
    /\ net' = [net EXCEPT !.cmdQ = Append(@, [t |-> "subRefresh", ch |-> c, id |-> 0, rec |-> FALSE, roff |-> 0, rep |-> 0])]
    /\ UNCHANGED <<srv, pr, sdk, mon, gh, viol>>

=============================================================================
