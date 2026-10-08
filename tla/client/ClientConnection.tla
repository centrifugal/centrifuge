-------------------------- MODULE ClientConnection --------------------------
(***************************************************************************)
(* Connection lifecycle of Client.tla: close() (concurrent closes), expiry *)
(* and refresh (expire timer, RefreshHandler, Client.Refresh), the stale   *)
(* timer, Client.Disconnect and slow writes.                               *)
(***************************************************************************)
EXTENDS ClientBase

-----------------------------------------------------------------------------
(* close(disconnect): several close goroutines may run, the first to take    *)
(* connectMu with the client not closed does the work, the others return.     *)
CL_StartClosing(p) ==  \* startWriter; c.closing.Store(true)
    LET r == pr[p] IN
    /\ IsClose(p) /\ r.pc = "start"
    /\ srv' = [srv EXCEPT !.closing = TRUE]
    /\ Step(p, [r EXCEPT !.pc = "lock"], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

CL_SetClosed(p) ==  \* connectMu.Lock(); statusClosed (or return); stopTimer; snapshot channels
    LET r == pr[p] IN
    /\ IsClose(p) /\ r.pc = "lock" /\ srv.connectMu = NoHolder
    /\ IF srv.status = "closed"
       THEN /\ Exit(p, <<>>) /\ UNCHANGED <<srv, gh>>
       ELSE /\ srv' = [srv EXCEPT !.status = "closed", !.connectMu = p,
                                  !.expTimer = FALSE, !.staleTimer = FALSE, !.presTimer = FALSE]
            /\ Step(p, [r EXCEPT !.pc = "push", !.prev = srv.status,
                                 !.snap = {c \in Chans : srv.chans[c].gen # 0}], <<>>)
            /\ gh' = [gh EXCEPT !.closePhase = "closing", !.closeReason = r.reason]
    /\ UNCHANGED <<brk, net, sdk, mon, viol>>

CL_WriteDisconnectPush(p) ==  \* the disconnect push (not when the transport itself closed)
    LET r == pr[p]
        \* DisconnectSlow closes the writer without flushing: the push is dropped
        push == IF r.reason \notin {"closed", "slow"} THEN <<Fr("disconnect", ConnectChan, 0, 0, 0, <<>>, r.reason)>> ELSE <<>>
    IN
    /\ IsClose(p) /\ r.pc = "push"
    \* writeDisconnect: the queue is closed, the remaining items and then the push
    \* are written as the last frame. DisconnectPushNotLast: the old order, push
    \* enqueued first and the writer closed later.
    /\ IF "DisconnectPushNotLast" \notin Bugs
       THEN /\ mon' = IF net.open THEN MonSeq(mon, push) ELSE mon
            /\ net' = IF net.open THEN [net EXCEPT !.wire = @ \o push, !.open = FALSE] ELSE net
            /\ Step(p, [r EXCEPT !.pc = "plock"], <<>>)
       ELSE /\ Emit(push)
            /\ Step(p, [r EXCEPT !.pc = "wclose"], <<>>)
    /\ UNCHANGED <<srv, brk, sdk, gh, viol>>

CL_CloseWriter(p) ==  \* messageWriter.close; transport.Close: nothing is written after this
    LET r == pr[p] IN
    /\ IsClose(p) /\ r.pc = "wclose"
    /\ net' = [net EXCEPT !.open = FALSE]
    /\ Step(p, [r EXCEPT !.pc = "plock"], <<>>)
    /\ UNCHANGED <<srv, brk, sdk, mon, gh, viol>>

CL_PresenceLock(p) ==  \* c.presenceMu.Lock(): after a presence tick in progress
    LET r == pr[p] IN
    /\ IsClose(p) /\ r.pc = "plock" /\ srv.presenceMu = NoHolder
    /\ srv' = [srv EXCEPT !.presenceMu = p]
    /\ Step(p, [r EXCEPT !.pc = "loop"], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

CL_UnsubscribeLoop(p) ==  \* for channel := range channels { unsubscribeWaiting(..., disconnect, ...) }
    LET r == pr[p] IN
    /\ IsClose(p) /\ r.pc = "loop"
    /\ IF r.snap = {}
       THEN Step(p, [r EXCEPT !.pc = "pending"], <<>>)
       ELSE \E c \in r.snap :
              Step(p, StartUW([r EXCEPT !.snap = @ \ {c}], c, "disconnect", FALSE, TRUE, 0, "loop"), <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

CL_DisconnectHandler(p) ==  \* waitPendingUnsubscribes; DisconnectHandler (if it was connected)
    LET r == pr[p] IN
    /\ IsClose(p) /\ r.pc = "pending"
    /\ srv.pendingUnsubs = 0 \/ Timeouts   \* 30s give-up with Timeouts
    /\ srv' = [srv EXCEPT !.connectMu = NoHolder, !.presenceMu = NoHolder]
    /\ gh' = [gh EXCEPT !.closePhase = "done", !.disc = (r.prev = "connected"),
                        !.discN = IF r.prev = "connected" THEN @ + 1 ELSE @]
    /\ Exit(p, <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, viol>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* Connection expiry and refresh. cred: "valid", "expired" (c.exp passed)  *)
(* or "none" (c.exp = 0). expTimer: the timer armed for timerOpExpire.     *)
(***************************************************************************)
ExpireTimerFires ==  \* onTimerOp(timerOpExpire) at c.nextExpire, c.exp has passed: expire()
    /\ srv.expTimer /\ srv.cred = "expired"
    /\ <<"exp", 0>> \notin DOMAIN pr
    /\ srv' = [srv EXCEPT !.expTimer = FALSE]
    /\ pr' = pr @@ (<<"exp", 0>> :> [pc |-> "check"])
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

EXP_Expire(p) ==  \* onTimerOp / expire(): status and c.exp under c.mu
    LET r == pr[p] IN
    /\ p[1] = "exp" /\ r.pc = "check"
    /\ CASE srv.status = "closed" \/ srv.cred = "none" ->
              Exit(p, <<>>) /\ UNCHANGED srv
         [] RefreshMode = "server" ->   \* RefreshHandler next, outside c.mu
              Step(p, [r EXCEPT !.pc = "handler"], <<>>)
         [] OTHER -> Step(p, [r EXCEPT !.pc = "chk"], <<>>)   \* checkExpired
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

EXP_RefreshHandler(p) ==  \* c.eventHub.refreshHandler(RefreshEvent{}, cb)
    LET r == pr[p] IN
    /\ p[1] = "exp" /\ r.pc = "handler"
    /\ brk' = [brk EXCEPT !.rcbs = @ + 1]
    /\ Exit(p, RcbSp("server"))
    /\ UNCHANGED <<srv, net, sdk, mon, gh, viol>>

(* The application's RefreshCallback, invoked at any time (or never). *)
RCB_RefreshCallback(p, k) ==
    LET r == pr[p] IN
    /\ p[1] = "rcb" /\ r.pc = "decide"
    /\ k = "extend" => brk.extends < MaxExtends
    /\ CASE k = "expired" ->   \* close(DisconnectExpired) / writeDisconnectOrErrorFlush
              Exit(p, CloseSp("expired")) /\ UNCHANGED srv
         [] k = "error" -> Exit(p, CloseSp("error")) /\ UNCHANGED srv
         [] r.kind = "server" ->  \* c.exp = expireAt (no status check), then checkExpired
              /\ srv' = [srv EXCEPT !.cred = "valid"]
              /\ Step(p, [r EXCEPT !.pc = "chk"], <<>>)
         [] OTHER ->              \* client refresh: c.exp, addExpireUpdate(scheduleNext), reply
              /\ srv' = [srv EXCEPT !.cred = "valid", !.expTimer = IF srv.status = "closed" THEN @ ELSE TRUE]
              /\ Step(p, [r EXCEPT !.pc = "reply"], <<>>)
    /\ brk' = IF k = "extend" THEN [brk EXCEPT !.extends = @ + 1] ELSE brk
    /\ UNCHANGED <<net, sdk, mon, gh, viol>>

RCB_WriteRefreshReply(p) ==
    LET r == pr[p] IN
    /\ p[1] = "rcb" /\ r.pc = "reply"
    /\ Emit(<<Fr("refreshReply", ConnectChan, 0, 0, 0, <<>>, "none")>>)
    /\ Exit(p, <<>>)
    /\ UNCHANGED <<srv, brk, sdk, gh, viol>>

CHK_CheckExpired(p) ==  \* checkExpired: status and c.exp under c.mu
    LET r == pr[p] IN
    /\ p[1] \in {"exp", "rcb"} /\ r.pc = "chk"
    /\ CASE srv.status = "closed" \/ srv.cred = "none" -> Exit(p, <<>>)
         [] srv.cred = "valid" ->   \* refreshed: server-side refresh re-arms the timer
              IF RefreshMode = "server" THEN Step(p, [r EXCEPT !.pc = "arm"], <<>>) ELSE Exit(p, <<>>)
         [] OTHER -> Exit(p, CloseSp("expired"))   \* _ = c.close(DisconnectExpired)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

CHK_RearmTimer(p) ==  \* c.mu.Lock(); if not closed: addExpireUpdate(ttl, true)
    LET r == pr[p] IN
    /\ p[1] \in {"exp", "rcb"} /\ r.pc = "arm"
    /\ srv' = IF srv.status = "closed" THEN srv ELSE [srv EXCEPT !.expTimer = TRUE]
    /\ Exit(p, <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

(* Client.Refresh by the application: expired -> go close(DisconnectExpired); *)
(* extend -> c.exp and the timer under c.mu, then the refresh push; no expiry  *)
(* -> c.exp = 0.                                                              *)
AREF_Update(p) ==
    LET r == pr[p] IN
    /\ p[1] = "aref" /\ r.pc = "start"
    /\ CASE r.kind = "expired" -> Exit(p, CloseSp("expired")) /\ UNCHANGED srv
         [] r.kind = "extend" ->
              /\ srv' = [srv EXCEPT !.cred = "valid", !.expTimer = IF srv.status = "closed" THEN @ ELSE TRUE]
              /\ Step(p, [r EXCEPT !.pc = "push"], <<>>)
         [] OTHER -> /\ srv' = [srv EXCEPT !.cred = "none"] /\ Step(p, [r EXCEPT !.pc = "push"], <<>>)
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

AREF_WriteRefreshPush(p) ==
    LET r == pr[p] IN
    /\ p[1] = "aref" /\ r.pc = "push"
    /\ Emit(<<Fr("refreshPush", ConnectChan, 0, 0, 0, <<>>, "none")>>)
    /\ Exit(p, <<>>)
    /\ UNCHANGED <<srv, brk, sdk, gh, viol>>

ServerRefresh(k) ==  \* the application calls Client.Refresh on a connected client
    /\ RefreshMode = "server" /\ srv.status = "connected" /\ brk.refreshes < MaxRefreshes
    /\ brk' = [brk EXCEPT !.refreshes = @ + 1]
    /\ pr' = pr @@ (<<"aref", brk.refreshes + 1>> :> [pc |-> "start", kind |-> k])
    /\ UNCHANGED <<srv, net, sdk, mon, gh, viol>>

TimeExpires ==  \* time passes c.exp
    /\ srv.cred = "valid"
    /\ srv' = [srv EXCEPT !.cred = "expired"]
    /\ UNCHANGED <<pr, brk, net, sdk, mon, gh, viol>>

StaleTimerFires ==  \* onTimerOp(timerOpStale): closeStale()
    /\ srv.staleTimer /\ <<"stale", 0>> \notin DOMAIN pr
    /\ srv' = [srv EXCEPT !.staleTimer = FALSE]
    /\ pr' = pr @@ (<<"stale", 0>> :> [pc |-> "check"])
    /\ UNCHANGED <<brk, net, sdk, mon, gh, viol>>

STALE_CloseStale(p) ==  \* closeStale: if not authenticated and not closed: close(DisconnectStale)
    LET r == pr[p] IN
    /\ p[1] = "stale" /\ r.pc = "check"
    /\ Exit(p, IF ~srv.authenticated /\ srv.status # "closed" THEN CloseSp("stale") ELSE <<>>)
    /\ UNCHANGED <<srv, brk, net, sdk, mon, gh, viol>>

ServerDisconnect ==  \* Client.Disconnect / Node.Disconnect: go c.close(DisconnectForceNoReconnect)
    /\ brk.disc < MaxDisconnects /\ srv.status # "closed"
    /\ brk' = [brk EXCEPT !.disc = @ + 1]
    /\ pr' = pr @@ CloseSp("force")
    /\ UNCHANGED <<srv, net, sdk, mon, gh, viol>>

SlowWriteDetected ==  \* a write finds the queue over the limit: spawnCloseUnlessClosing(DisconnectSlow)
    /\ SlowWrite /\ ~brk.slow /\ net.open
    /\ brk' = [brk EXCEPT !.slow = TRUE]
    /\ pr' = pr @@ (IF srv.closing THEN <<>> ELSE CloseSp("slow"))
    /\ UNCHANGED <<srv, net, sdk, mon, gh, viol>>

SDK_Refresh ==  \* client-side refresh: the SDK sends a refresh command with a new token
    /\ RefreshMode = "client" /\ sdk.up /\ ~sdk.gone /\ brk.refreshes < MaxRefreshes
    /\ brk' = [brk EXCEPT !.refreshes = @ + 1]
    /\ net' = [net EXCEPT !.cmdQ = Append(@, [t |-> "refresh", ch |-> ConnectChan, id |-> 0, rec |-> FALSE, roff |-> 0, rep |-> 0])]
    /\ UNCHANGED <<srv, pr, sdk, mon, gh, viol>>

=============================================================================
