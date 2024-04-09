package centrifuge

import (
	"time"
)

// channelCache is responsible for keeping last publication and stream position in channel.
// It should periodically sync its state with a Broker if there were no new publications for a long time.
// It should check continuity in channel stream, if it's broken it should take appropriate actions.
// It should also handle delta flag correctly.
// When on it must be used by clients to check proper stream position in channel.
type channelCache struct {
	channel string
	node    *Node
	options ChannelCacheOptions
	closeCh chan struct{}
	pubCh   chan latestPub

	// prevPublication is a previous publication in channel.
	prevPublication *Publication
	// prevStreamPosition is a stream position of previous publication.
	prevStreamPosition StreamPosition

	latestPub latestPub
}

func newChannelCache(
	channel string,
	node *Node,
	options ChannelCacheOptions,
) *channelCache {
	c := &channelCache{
		channel: channel,
		node:    node,
		options: options,
		closeCh: make(chan struct{}),
		pubCh:   make(chan latestPub),
	}
	return c
}

type latestPub struct {
	pub *Publication
	sp  StreamPosition
	// TODO: probably it's unnecessary if we load and cache prev publication anyway? Publisher can publish
	// without delta flag?
	delta bool
}

func (c *channelCache) initState(latestPublication *Publication, currentStreamPosition StreamPosition) {
	c.prevStreamPosition = currentStreamPosition
	c.prevPublication = latestPublication
	go c.run()
}

func (c *channelCache) handlePublication(pub *Publication, sp StreamPosition, delta bool, _ *Publication) {
	select {
	case c.pubCh <- latestPub{pub: pub, sp: sp, delta: delta}:
	case <-c.closeCh:
	}
}

func (c *channelCache) broadcast() {
	_ = c.node.handlePublication(c.channel, c.latestPub.pub, c.latestPub.sp, c.latestPub.delta, c.prevPublication, true)
	c.prevPublication = c.latestPub.pub
	c.prevStreamPosition = c.latestPub.sp
}

func (c *channelCache) setLatestPub(cp latestPub) {
	c.latestPub = cp
}

func (c *channelCache) run() {
	t := time.NewTimer(c.options.Delay)
	t.Stop()
	defer t.Stop()
	scheduled := false
	for {
		select {
		case <-c.closeCh:
			return
		case cp := <-c.pubCh:
			c.setLatestPub(cp)
			if c.options.Delay == 0 {
				c.broadcast()
				continue
			}
			if !scheduled {
				t.Reset(c.options.Delay)
				scheduled = true
			}
		case <-t.C:
			c.broadcast()
			scheduled = false
		}
	}
}

func (c *channelCache) close() {
	close(c.closeCh)
}
