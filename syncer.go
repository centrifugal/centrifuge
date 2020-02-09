package centrifuge

import (
	"sync"
	"sync/atomic"

	"github.com/centrifugal/protocol"
)

type pubSubSyncer struct {
	subSyncMu sync.RWMutex
	subSync   map[string]*subscribeState
}

func newPubSubSyncer() *pubSubSyncer {
	return &pubSubSyncer{
		subSync: make(map[string]*subscribeState),
	}
}

type subscribeState struct {
	// The following fields help us to synchronize PUB/SUB and history messages
	// during publication recovery process in channel.
	inSubscribe uint32
	pubBufferMu sync.Mutex
	pubBuffer   []*Publication
}

func (c *pubSubSyncer) isInSubscribe(channel string) bool {
	c.subSyncMu.RLock()
	defer c.subSyncMu.RUnlock()
	s, ok := c.subSync[channel]
	if !ok {
		return false
	}
	return atomic.LoadUint32(&s.inSubscribe) == 1
}

func (c *pubSubSyncer) setInSubscribe(channel string, flag bool) {
	c.subSyncMu.Lock()
	defer c.subSyncMu.Unlock()
	if flag {
		s := &subscribeState{}
		c.subSync[channel] = s
		atomic.StoreUint32(&s.inSubscribe, 1)
	} else {
		s, ok := c.subSync[channel]
		if !ok {
			return
		}
		atomic.StoreUint32(&s.inSubscribe, 0)
	}
}

func (c *pubSubSyncer) lockPublications(channel string) {
	c.subSyncMu.RLock()
	s, ok := c.subSync[channel]
	if !ok {
		c.subSyncMu.RUnlock()
		return
	}
	c.subSyncMu.RUnlock()
	s.pubBufferMu.Lock()
}

func (c *pubSubSyncer) unlockPublications(channel string, final bool) {
	c.subSyncMu.RLock()
	s, ok := c.subSync[channel]
	if !ok {
		c.subSyncMu.RUnlock()
		return
	}
	c.subSyncMu.RUnlock()
	s.pubBufferMu.Unlock()
	if final {
		c.subSyncMu.Lock()
		delete(c.subSync, channel)
		c.subSyncMu.Unlock()
	}
}

func (c *pubSubSyncer) appendToBufferedPubs(channel string, pub *protocol.Publication) {
	c.subSyncMu.RLock()
	defer c.subSyncMu.RUnlock()
	s := c.subSync[channel]
	s.pubBuffer = append(s.pubBuffer, pub)
}

func (c *pubSubSyncer) readBufferedPubs(channel string) []*Publication {
	c.subSyncMu.RLock()
	defer c.subSyncMu.RUnlock()
	s := c.subSync[channel]
	pubs := make([]*Publication, len(s.pubBuffer))
	copy(pubs, s.pubBuffer)
	s.pubBuffer = nil
	return pubs
}
