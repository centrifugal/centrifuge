package centrifuge

import (
	"slices"
	"time"

	"github.com/centrifugal/protocol"
)

// handleSharedPollSubscribe handles subscribe requests for shared poll channels (type=4).
// Lightweight subscribe: no broker, no hub, no positioning/recovery.
func (c *Client) handleSharedPollSubscribe(req *protocol.SubscribeRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	channel := req.Channel

	if c.eventHub.subscribeHandler == nil {
		return ErrorNotAvailable
	}

	var subscribingCh chan struct{}
	var subGen uint64
	var waitDeadline time.Time
	for {
		// Pre-register channel to track duplicate subscriptions (matches regular subscribe
		// flow). Reject if the channel is already reserved (subscribed OR a subscribe/map
		// subscribe in flight), mirroring validateSubscribeRequest — this keeps the
		// one-in-flight-subscribe-per-channel invariant. The reservation carries a
		// subscribingCh so a concurrent unsubscribe waits for this in-flight subscribe
		// instead of racing it: without it the unsubscribe removes the empty reservation
		// and the async finalize re-adds the channel, leaking the subscription.
		c.mu.Lock()
		if _, ok := c.channels[channel]; ok {
			c.mu.Unlock()
			return ErrorAlreadySubscribed
		}
		if _, ok := c.mapSubscribing[channel]; ok {
			c.mu.Unlock()
			return ErrorAlreadySubscribed
		}
		if c.mapSubscribePendingLocked(channel) {
			c.mu.Unlock()
			return ErrorAlreadySubscribed
		}
		channelLimit := c.node.config.ClientChannelLimit
		numChannels := c.numChannelsLocked()
		if channelLimit > 0 && numChannels >= channelLimit {
			c.mu.Unlock()
			return ErrorLimitExceeded
		}
		if c.status == statusClosed {
			// SubscribeHandler is not called on a closed client.
			c.mu.Unlock()
			return DisconnectConnectionClosed
		}
		if c.mustWaitAttemptEndsLocked(channel, &waitDeadline) {
			c.mu.Unlock()
			c.waitAttemptEnds(channel, waitDeadline)
			continue
		}
		subscribingCh = make(chan struct{})
		subGen = c.subGenCounter.Add(1)
		c.channels[channel] = ChannelContext{subscribingCh: subscribingCh, subGen: subGen}
		c.mu.Unlock()
		break
	}

	event := SubscribeEvent{
		Channel: channel,
		Token:   req.Token,
		Data:    req.Data,
		Type:    SubscriptionTypeSharedPoll,
	}

	answered := false // Guarded by c.mu.
	c.eventHub.subscribeHandler(event, func(reply SubscribeReply, err error) {
		// From here on every failure ends an allowed attempt through the
		// reservation removal. A closed client is handled when installing the
		// subscription below.
		if _, ok := c.answerSubscribeCallback(channel, subGen, err == nil, &answered); !ok {
			return
		}
		if err != nil {
			c.onSubscribeErrorGen(channel, subGen)
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, err, started, rw)
			return
		}
		res := &protocol.SubscribeResult{}
		res.Type = int32(SubscriptionTypeSharedPoll)

		// Shared poll subscriptions are always refreshed client-side.
		expires, ttl, expired := subscriptionExpiration(reply.Options.ExpireAt, true)
		if expired {
			c.onSubscribeErrorGen(channel, subGen)
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, ErrorExpired, started, rw)
			return
		}
		res.Expires = expires
		res.Ttl = ttl

		// Delta negotiation.
		var deltaType DeltaType
		if req.Delta != "" {
			dt := DeltaType(req.Delta)
			if slices.Contains(reply.Options.AllowedDeltaTypes, dt) {
				res.Delta = true
				deltaType = dt
			}
		}

		// Register channel with flagKeyed | flagSubscribed | flagClientSideRefresh.
		// flagDeltaAllowed is set unconditionally because keyed channels manage
		// per-key delta readiness in keyedWritePublication — the channel-level
		// first-full-then-delta progression does not apply.
		flags := flagSubscribed | flagKeyed | flagClientSideRefresh | flagDeltaAllowed
		if reply.Options.EmitPresence {
			flags |= flagEmitPresence
		}
		if reply.Options.EmitJoinLeave {
			flags |= flagEmitJoinLeave
		}
		if reply.Options.PushJoinLeave {
			flags |= flagPushJoinLeave
		}
		if reply.Options.MapClientPresenceChannel != "" {
			flags |= flagMapClientPresence
		}
		if reply.Options.MapUserPresenceChannel != "" {
			flags |= flagMapUserPresence
		}

		opts, ok := c.node.config.SharedPoll.GetSharedPollChannelOptions(channel)
		if !ok {
			// Ends the attempt, unless an unsubscribe removed it first and reported
			// it.
			c.onSubscribeErrorGen(channel, subGen)
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, ErrorNotAvailable, started, rw)
			return
		}
		if c.node.sharedPollManager != nil {
			res.Epoch = c.node.sharedPollManager.Epoch(channel, opts.isVersionless())
		}
		protoReply, err := c.getSubscribeCommandReply(res)
		if err != nil {
			c.onSubscribeErrorGen(channel, subGen)
			c.logWriteInternalErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, err, "error encoding subscribe", started, rw)
			return
		}

		// Write the reply while the reservation still makes an unsubscribe of the
		// channel wait for this subscribe (as the regular subscribe writes its
		// reply before its commit): an unsubscribe must not remove the
		// subscription, or reply, before the subscribe reply. Nothing of the
		// channel reaches the client before the subscription is installed below.
		c.mu.RLock()
		resv, haveResv := c.channels[channel]
		replied := haveResv && resv.subGen == subGen && c.status != statusClosed
		c.mu.RUnlock()
		if replied {
			c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
			c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, channel)
		}
		c.releaseSubscribeCommandReply(protoReply)

		// Take the reservation's wait-gate channel from the map rather than using
		// the local captured at reservation time: the unsubscribe wait-gate timeout
		// path closes it and nils it in the stored context, so closing the local
		// again would panic. A nil here means that already happened.
		c.mu.Lock()
		resv, haveResv = c.channels[channel]
		if !haveResv || resv.subGen != subGen {
			// Reservation lost — a subscribe stalled past the unsubscribe wait-gate
			// timeout, and the channel may now belong to a fresh subscribe. Installing
			// our live context would orphan that reservation's subscribingCh (its
			// waiters burn the full 5s timeout) and resurrect a channel this client
			// already unsubscribed from. Nothing of ours is installed yet — shared
			// poll uses no broker and no hub — so there is nothing else to undo.
			// The attempt was ended by whoever removed the reservation (or by
			// answerSubscribeCallback, if it was gone already).
			c.mu.Unlock()
			if !replied {
				c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, ErrorInternal, started, rw)
			}
			return
		}
		gateCh := resv.subscribingCh
		if c.status == statusClosed {
			// Client closed mid-subscribe: drop our reservation and release any
			// unsubscribe waiting on the gate (mirrors commitSubscription's closed
			// path).
			delete(c.channels, channel)
			if resv.subscribeAllowed {
				c.addAttemptEndLocked(channel)
			}
			c.mu.Unlock()
			if resv.subscribeAllowed {
				c.endSubscribeAttempt(channel, nil, nil)
			}
			if gateCh != nil {
				close(gateCh)
			}
			if !replied {
				// Only completes the command (event, metrics): the transport is closed.
				c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, DisconnectConnectionClosed, started, rw)
			}
			return
		}
		chCtx := ChannelContext{
			flags:                    flags,
			expireAt:                 reply.Options.ExpireAt,
			info:                     reply.Options.ChannelInfo,
			mapClientPresenceChannel: reply.Options.MapClientPresenceChannel,
			mapUserPresenceChannel:   reply.Options.MapUserPresenceChannel,
			subGen:                   subGen,
		}
		if reply.Options.EmitPresence || reply.Options.EmitJoinLeave || reply.Options.MapClientPresenceChannel != "" {
			// Presence and join are done after the install below.
			chCtx.joinGate = &joinGate{}
		}
		c.channels[channel] = chCtx
		if c.keyed == nil {
			c.keyed = &keyedState{
				channels:    make(map[string]*keyedChannelDeltaState),
				trackedKeys: make(map[string]map[string]*keyedKeyState),
			}
		}
		if deltaType != deltaTypeNone {
			c.keyed.channels[channel] = &keyedChannelDeltaState{deltaType: deltaType}
		}
		c.mu.Unlock()
		// Release any unsubscribe waiting on the in-flight subscribe only after all
		// subscription state (channel context, keyed delta state, and the map
		// presence set up below) is installed, so a woken unsubscribe tears down the
		// complete subscription rather than a partial one. The stored context above
		// carries no subscribingCh, so the timeout path can no longer close it and
		// this is the sole closer.
		if gateCh != nil {
			defer close(gateCh)
		}
		// After the presence and join below: an unsubscribe may have left its
		// presence removal and leave to it.
		defer c.finishJoin(channel, chCtx)

		c.node.keyedManager.getOrCreateChannel(channel, opts.toKeyedChannelOptions())
		c.setupMapPresenceAndJoin(channel, reply.Options)
	})
	return nil
}
