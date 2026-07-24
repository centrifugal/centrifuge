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
	channelLimit := c.node.config.ClientChannelLimit
	numChannels := len(c.channels) + len(c.mapSubscribing)
	if channelLimit > 0 && numChannels >= channelLimit {
		c.mu.Unlock()
		return ErrorLimitExceeded
	}
	subscribingCh := make(chan struct{})
	subGen := c.subGenCounter.Add(1)
	c.channels[channel] = ChannelContext{subscribingCh: subscribingCh, subGen: subGen}
	c.mu.Unlock()

	event := SubscribeEvent{
		Channel: channel,
		Token:   req.Token,
		Data:    req.Data,
		Type:    SubscriptionTypeSharedPoll,
	}

	c.eventHub.subscribeHandler(event, func(reply SubscribeReply, err error) {
		if err != nil {
			c.onSubscribeErrorGen(channel, subGen)
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, err, started, rw)
			return
		}

		res := &protocol.SubscribeResult{}
		res.Type = int32(SubscriptionTypeSharedPoll)

		if reply.Options.ExpireAt > 0 {
			ttl := reply.Options.ExpireAt - time.Now().Unix()
			if ttl <= 0 {
				c.onSubscribeErrorGen(channel, subGen)
				c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, ErrorExpired, started, rw)
				return
			}
			res.Expires = true
			res.Ttl = uint32(ttl)
		}

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

		// Take the reservation's wait-gate channel from the map rather than using
		// the local captured at reservation time: the unsubscribe wait-gate timeout
		// path closes it and nils it in the stored context, so closing the local
		// again would panic. A nil here means that already happened.
		c.mu.Lock()
		resv, haveResv := c.channels[channel]
		if !haveResv || resv.subGen != subGen {
			// Reservation lost — a subscribe stalled past the unsubscribe wait-gate
			// timeout, and the channel may now belong to a fresh subscribe. Installing
			// our live context would orphan that reservation's subscribingCh (its
			// waiters burn the full 5s timeout) and resurrect a channel this client
			// already unsubscribed from. Nothing of ours is installed yet — shared
			// poll uses no broker and no hub — so there is nothing else to undo.
			c.mu.Unlock()
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, ErrorInternal, started, rw)
			return
		}
		gateCh := resv.subscribingCh
		if c.status == statusClosed {
			// Client closed mid-subscribe: drop our reservation and release any
			// unsubscribe waiting on the gate (mirrors commitSubscription's closed
			// path).
			delete(c.channels, channel)
			c.mu.Unlock()
			if gateCh != nil {
				close(gateCh)
			}
			return
		}
		c.channels[channel] = ChannelContext{
			flags:                    flags,
			expireAt:                 reply.Options.ExpireAt,
			info:                     reply.Options.ChannelInfo,
			mapClientPresenceChannel: reply.Options.MapClientPresenceChannel,
			mapUserPresenceChannel:   reply.Options.MapUserPresenceChannel,
			subGen:                   subGen,
		}
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

		// Ensure keyed channel state exists.
		opts, ok := c.node.config.SharedPoll.GetSharedPollChannelOptions(channel)
		if !ok {
			// cleanupKeyed first: the keyed delta state installed above is only
			// reachable while the channel is in c.channels (unsubscribe returns
			// early once the entry is gone), so dropping the reservation first
			// would strand it for the lifetime of the connection.
			c.cleanupKeyed(channel)
			c.onSubscribeErrorGen(channel, subGen)
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, ErrorNotAvailable, started, rw)
			return
		}
		if c.node.sharedPollManager != nil {
			res.Epoch = c.node.sharedPollManager.Epoch(channel, opts.isVersionless())
		}
		keyedOpts := opts.toKeyedChannelOptions()
		c.node.keyedManager.getOrCreateChannel(channel, keyedOpts)

		protoReply, err := c.getSubscribeCommandReply(res)
		if err != nil {
			c.logWriteInternalErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, err, "error encoding subscribe", started, rw)
			return
		}
		c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, channel)
		c.releaseSubscribeCommandReply(protoReply)

		c.setupMapPresenceAndJoin(channel, reply.Options)
	})
	return nil
}
