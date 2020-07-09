package centrifuge

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
)

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

var bufferPool = sync.Pool{
	// New is called when a new instance is needed
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

func putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

// pubSubSync wraps logic to synchronize recovery with PUB/SUB.
type pubSubSync struct {
	subSyncMu sync.RWMutex
	subSync   map[string]*subscribeState
}

// NewPubSubSync creates new PubSubSyncer.
func newPubSubSync() *pubSubSync {
	return &pubSubSync{
		subSync: make(map[string]*subscribeState),
	}
}

type subscribeState struct {
	// The following fields help us to synchronize PUB/SUB and history messages
	// during publication recovery process in channel.
	inSubscribe     uint32
	pubBufferMu     sync.Mutex
	pubBufferLocked bool
	pubBuffer       []*Publication
}

// SyncPublication ...
func (c *pubSubSync) SyncPublication(channel string, pub *Publication, syncedFn func()) {
	if c.isInSubscribe(channel) {
		// Client currently in process of subscribing to this channel. In this case we keep
		// publications in slice buffer. Publications from this temporary buffer will be sent in
		// subscribe reply.
		c.LockBuffer(channel)
		if c.isInSubscribe(channel) {
			// Sync point not reached yet - put Publication to tmp slice.
			c.appendPubToBuffer(channel, pub)
			c.unlockBuffer(channel)
			return
		}
		// Sync point already passed - send Publication into connection.
		c.unlockBuffer(channel)
	}
	syncedFn()
}

// StartBuffering ...
func (c *pubSubSync) StartBuffering(channel string) {
	c.subSyncMu.Lock()
	defer c.subSyncMu.Unlock()
	s := &subscribeState{}
	c.subSync[channel] = s
	atomic.StoreUint32(&s.inSubscribe, 1)
}

// StopBuffering ...
func (c *pubSubSync) StopBuffering(channel string) {
	c.subSyncMu.Lock()
	defer c.subSyncMu.Unlock()
	s, ok := c.subSync[channel]
	if !ok {
		return
	}
	atomic.StoreUint32(&s.inSubscribe, 0)
	if s.pubBufferLocked {
		s.pubBufferMu.Unlock()
	}
	delete(c.subSync, channel)
}

func (c *pubSubSync) isInSubscribe(channel string) bool {
	c.subSyncMu.RLock()
	defer c.subSyncMu.RUnlock()
	s, ok := c.subSync[channel]
	if !ok {
		return false
	}
	return atomic.LoadUint32(&s.inSubscribe) == 1
}

// LockBuffer ...
func (c *pubSubSync) LockBuffer(channel string) {
	c.subSyncMu.Lock()
	s, ok := c.subSync[channel]
	if !ok {
		c.subSyncMu.Unlock()
		return
	}
	s.pubBufferLocked = true
	c.subSyncMu.Unlock()
	s.pubBufferMu.Lock()
}

// UnlockBuffer ...
func (c *pubSubSync) unlockBuffer(channel string) {
	c.subSyncMu.Lock()
	defer c.subSyncMu.Unlock()
	s, ok := c.subSync[channel]
	if !ok {
		return
	}
	if s.pubBufferLocked {
		s.pubBufferMu.Unlock()
	}
}

func (c *pubSubSync) appendPubToBuffer(channel string, pub *Publication) {
	c.subSyncMu.RLock()
	defer c.subSyncMu.RUnlock()
	s := c.subSync[channel]
	s.pubBuffer = append(s.pubBuffer, pub)
}

// ReadBuffered ...
func (c *pubSubSync) ReadBuffered(channel string) []*Publication {
	c.subSyncMu.RLock()
	defer c.subSyncMu.RUnlock()
	s := c.subSync[channel]
	pubs := make([]*Publication, len(s.pubBuffer))
	copy(pubs, s.pubBuffer)
	s.pubBuffer = nil
	return pubs
}

// MergePublications allows to merge recovered pubs with buffered pubs
// collected during extracting recovered so result is ordered and with
// duplicates removed.
func MergePublications(recoveredPubs []*Publication, bufferedPubs []*Publication, isLegacyOrder bool) ([]*Publication, bool) {
	if len(bufferedPubs) > 0 {
		recoveredPubs = append(recoveredPubs, bufferedPubs...)
	}
	if isLegacyOrder {
		sort.Slice(recoveredPubs, func(i, j int) bool {
			return recoveredPubs[i].Offset > recoveredPubs[j].Offset
		})
	} else {
		sort.Slice(recoveredPubs, func(i, j int) bool {
			return recoveredPubs[i].Offset < recoveredPubs[j].Offset
		})
	}
	if len(bufferedPubs) > 0 {
		if len(recoveredPubs) > 1 {
			recoveredPubs = uniquePublications(recoveredPubs)
		}
		prevOffset := recoveredPubs[0].Offset
		for _, p := range recoveredPubs[1:] {
			pubOffset := p.Offset
			var isWrongOffset bool
			if isLegacyOrder {
				isWrongOffset = pubOffset != prevOffset-1
			} else {
				isWrongOffset = pubOffset != prevOffset+1
			}
			if isWrongOffset {
				return nil, false
			}
			prevOffset = pubOffset
		}
	}
	return recoveredPubs, true
}

// uniquePublications returns slice of unique Publications.
func uniquePublications(s []*Publication) []*Publication {
	keys := make(map[uint64]struct{})
	list := make([]*Publication, 0, len(s))
	for _, entry := range s {
		val := entry.Offset
		if _, value := keys[val]; !value {
			keys[val] = struct{}{}
			list = append(list, entry)
		}
	}
	return list
}
