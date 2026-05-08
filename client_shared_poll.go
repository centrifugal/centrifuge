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

	// Pre-register channel to track duplicate subscriptions (matches regular subscribe flow).
	c.mu.Lock()
	if ctx, ok := c.channels[channel]; ok && channelHasFlag(ctx.flags, flagSubscribed) {
		c.mu.Unlock()
		return ErrorAlreadySubscribed
	}
	channelLimit := c.node.config.ClientChannelLimit
	numChannels := len(c.channels) + len(c.mapSubscribing)
	if channelLimit > 0 && numChannels >= channelLimit {
		c.mu.Unlock()
		return ErrorLimitExceeded
	}
	c.channels[channel] = ChannelContext{}
	c.mu.Unlock()

	event := SubscribeEvent{
		Channel: channel,
		Token:   req.Token,
		Data:    req.Data,
		Type:    SubscriptionTypeSharedPoll,
	}

	c.eventHub.subscribeHandler(event, func(reply SubscribeReply, err error) {
		if err != nil {
			c.onSubscribeError(channel)
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, err, started, rw)
			return
		}

		res := &protocol.SubscribeResult{}
		res.Type = int32(SubscriptionTypeSharedPoll)

		if reply.Options.ExpireAt > 0 {
			ttl := reply.Options.ExpireAt - time.Now().Unix()
			if ttl <= 0 {
				c.onSubscribeError(channel)
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

		c.mu.Lock()
		if c.status == statusClosed {
			c.mu.Unlock()
			return
		}
		c.channels[channel] = ChannelContext{
			flags:                    flags,
			expireAt:                 reply.Options.ExpireAt,
			info:                     reply.Options.ChannelInfo,
			mapClientPresenceChannel: reply.Options.MapClientPresenceChannel,
			mapUserPresenceChannel:   reply.Options.MapUserPresenceChannel,
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

		// Ensure keyed channel state exists.
		opts, ok := c.node.config.SharedPoll.GetSharedPollChannelOptions(channel)
		if !ok {
			c.onSubscribeError(channel)
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
