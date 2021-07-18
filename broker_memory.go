package centrifuge

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/memstream"
	"github.com/centrifugal/centrifuge/internal/priority"
)

// MemoryBroker is builtin default Broker which allows to run Centrifuge-based
// server without any external broker. All data managed inside process memory.
//
// With this Broker you can only run single Centrifuge node. If you need to scale
// you should consider using another Broker implementation instead – for example
// RedisBroker.
//
// Running single node can be sufficient for many use cases especially when you
// need maximum performance and not too many online clients. Consider configuring
// your load balancer to have one backup Centrifuge node for HA in this case.
type MemoryBroker struct {
	node         *Node
	historyHub   *historyHub
	eventHandler BrokerEventHandler

	// pubLocks synchronize access to publishing. We have to sync publish
	// to handle publications in the order of offset to prevent InsufficientState
	// errors.
	// TODO: maybe replace with sharded pool of workers with buffered channels.
	pubLocks map[int]*sync.Mutex
}

var _ Broker = (*MemoryBroker)(nil)

// MemoryBrokerConfig is a memory broker config.
type MemoryBrokerConfig struct {
	// HistoryMetaTTL sets a time of inactive stream meta information expiration.
	// This information contains an epoch and offset of each stream. Having this
	// meta information helps in message recovery process.
	// Must have a reasonable value for application.
	// At moment works with seconds precision.
	// TODO v1: since we have epoch, things should also properly work without meta
	// information at all (but we loose possibility of long-term recover in stream
	// without new messages). We can make this optional and disabled by default at
	// least.
	HistoryMetaTTL time.Duration
}

const numPubLocks = 4096

// NewMemoryBroker initializes MemoryBroker.
func NewMemoryBroker(n *Node, c MemoryBrokerConfig) (*MemoryBroker, error) {
	pubLocks := make(map[int]*sync.Mutex, numPubLocks)
	for i := 0; i < numPubLocks; i++ {
		pubLocks[i] = &sync.Mutex{}
	}
	b := &MemoryBroker{
		node:       n,
		historyHub: newHistoryHub(c.HistoryMetaTTL),
		pubLocks:   pubLocks,
	}
	return b, nil
}

// Run runs memory broker.
func (b *MemoryBroker) Run(h BrokerEventHandler) error {
	b.eventHandler = h
	b.historyHub.runCleanups()
	return nil
}

// Close is noop for now.
func (b *MemoryBroker) Close(_ context.Context) error {
	return nil
}

func (b *MemoryBroker) pubLock(ch string) *sync.Mutex {
	return b.pubLocks[index(ch, numPubLocks)]
}

// Publish adds message into history hub and calls node method to handle message.
// We don't have any PUB/SUB here as Memory Engine is single node only.
func (b *MemoryBroker) Publish(ch string, data []byte, opts PublishOptions) (StreamPosition, error) {
	mu := b.pubLock(ch)
	mu.Lock()
	defer mu.Unlock()

	pub := &Publication{
		Data: data,
		Info: opts.ClientInfo,
	}
	if opts.HistorySize > 0 && opts.HistoryTTL > 0 {
		streamTop, err := b.historyHub.add(ch, pub, opts)
		if err != nil {
			return StreamPosition{}, err
		}
		pub.Offset = streamTop.Offset
		return streamTop, b.eventHandler.HandlePublication(ch, pub, streamTop)
	}
	return StreamPosition{}, b.eventHandler.HandlePublication(ch, pub, StreamPosition{})
}

// PublishJoin - see Broker interface description.
func (b *MemoryBroker) PublishJoin(ch string, info *ClientInfo) error {
	return b.eventHandler.HandleJoin(ch, info)
}

// PublishLeave - see Broker interface description.
func (b *MemoryBroker) PublishLeave(ch string, info *ClientInfo) error {
	return b.eventHandler.HandleLeave(ch, info)
}

// PublishControl - see Broker interface description.
func (b *MemoryBroker) PublishControl(data []byte, _, _ string) error {
	return b.eventHandler.HandleControl(data)
}

// Subscribe is noop here.
func (b *MemoryBroker) Subscribe(_ string) error {
	return nil
}

// Unsubscribe node from channel. Noop here.
func (b *MemoryBroker) Unsubscribe(_ string) error {
	return nil
}

// History - see Broker interface description.
func (b *MemoryBroker) History(ch string, filter HistoryFilter) ([]*Publication, StreamPosition, error) {
	return b.historyHub.get(ch, filter)
}

// RemoveHistory - see Broker interface description.
func (b *MemoryBroker) RemoveHistory(ch string) error {
	return b.historyHub.remove(ch)
}

type historyHub struct {
	sync.RWMutex
	streams         map[string]*memstream.Stream
	nextExpireCheck int64
	expireQueue     priority.Queue
	expires         map[string]int64
	historyMetaTTL  time.Duration
	nextRemoveCheck int64
	removeQueue     priority.Queue
	removes         map[string]int64
}

func newHistoryHub(historyMetaTTL time.Duration) *historyHub {
	return &historyHub{
		streams:        make(map[string]*memstream.Stream),
		expireQueue:    priority.MakeQueue(),
		expires:        make(map[string]int64),
		historyMetaTTL: historyMetaTTL,
		removeQueue:    priority.MakeQueue(),
		removes:        make(map[string]int64),
	}
}

func (h *historyHub) runCleanups() {
	go h.expireStreams()
	if h.historyMetaTTL > 0 {
		go h.removeStreams()
	}
}

func (h *historyHub) removeStreams() {
	var nextRemoveCheck int64
	for {
		time.Sleep(time.Second)
		h.Lock()
		if h.nextRemoveCheck == 0 || h.nextRemoveCheck > time.Now().Unix() {
			h.Unlock()
			continue
		}
		nextRemoveCheck = 0
		for h.removeQueue.Len() > 0 {
			item := heap.Pop(&h.removeQueue).(*priority.Item)
			expireAt := item.Priority
			if expireAt > time.Now().Unix() {
				heap.Push(&h.removeQueue, item)
				nextRemoveCheck = expireAt
				break
			}
			ch := item.Value
			exp, ok := h.removes[ch]
			if !ok {
				continue
			}
			if exp <= expireAt {
				delete(h.removes, ch)
				delete(h.streams, ch)
			} else {
				heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: exp})
			}
		}
		h.nextRemoveCheck = nextRemoveCheck
		h.Unlock()
	}
}

func (h *historyHub) expireStreams() {
	var nextExpireCheck int64
	for {
		time.Sleep(time.Second)
		h.Lock()
		if h.nextExpireCheck == 0 || h.nextExpireCheck > time.Now().Unix() {
			h.Unlock()
			continue
		}
		nextExpireCheck = 0
		for h.expireQueue.Len() > 0 {
			item := heap.Pop(&h.expireQueue).(*priority.Item)
			expireAt := item.Priority
			if expireAt > time.Now().Unix() {
				heap.Push(&h.expireQueue, item)
				nextExpireCheck = expireAt
				break
			}
			ch := item.Value
			exp, ok := h.expires[ch]
			if !ok {
				continue
			}
			if exp <= expireAt {
				delete(h.expires, ch)
				if stream, ok := h.streams[ch]; ok {
					stream.Clear()
				}
			} else {
				heap.Push(&h.expireQueue, &priority.Item{Value: ch, Priority: exp})
			}
		}
		h.nextExpireCheck = nextExpireCheck
		h.Unlock()
	}
}

func (h *historyHub) add(ch string, pub *Publication, opts PublishOptions) (StreamPosition, error) {
	h.Lock()
	defer h.Unlock()

	var index uint64
	var epoch string

	expireAt := time.Now().Unix() + int64(opts.HistoryTTL.Seconds())
	if _, ok := h.expires[ch]; !ok {
		heap.Push(&h.expireQueue, &priority.Item{Value: ch, Priority: expireAt})
	}
	h.expires[ch] = expireAt
	if h.nextExpireCheck == 0 || h.nextExpireCheck > expireAt {
		h.nextExpireCheck = expireAt
	}

	if h.historyMetaTTL > 0 {
		removeAt := time.Now().Unix() + int64(h.historyMetaTTL.Seconds())
		if _, ok := h.removes[ch]; !ok {
			heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: removeAt})
		}
		h.removes[ch] = removeAt
		if h.nextRemoveCheck == 0 || h.nextRemoveCheck > removeAt {
			h.nextRemoveCheck = removeAt
		}
	}

	if stream, ok := h.streams[ch]; ok {
		index, _ = stream.Add(pub, opts.HistorySize)
		epoch = stream.Epoch()
	} else {
		stream := memstream.New()
		index, _ = stream.Add(pub, opts.HistorySize)
		epoch = stream.Epoch()
		h.streams[ch] = stream
	}
	pub.Offset = index

	return StreamPosition{Offset: index, Epoch: epoch}, nil
}

// Lock must be held outside.
func (h *historyHub) createStream(ch string) StreamPosition {
	stream := memstream.New()
	h.streams[ch] = stream
	streamPosition := StreamPosition{}
	streamPosition.Offset = 0
	streamPosition.Epoch = stream.Epoch()
	return streamPosition
}

func getPosition(stream *memstream.Stream) StreamPosition {
	streamPosition := StreamPosition{}
	streamPosition.Offset = stream.Top()
	streamPosition.Epoch = stream.Epoch()
	return streamPosition
}

func (h *historyHub) get(ch string, filter HistoryFilter) ([]*Publication, StreamPosition, error) {
	h.Lock()
	defer h.Unlock()

	if h.historyMetaTTL > 0 {
		removeAt := time.Now().Unix() + int64(h.historyMetaTTL.Seconds())
		if _, ok := h.removes[ch]; !ok {
			heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: removeAt})
		}
		h.removes[ch] = removeAt
		if h.nextRemoveCheck == 0 || h.nextRemoveCheck > removeAt {
			h.nextRemoveCheck = removeAt
		}
	}

	stream, ok := h.streams[ch]
	if !ok {
		return nil, h.createStream(ch), nil
	}

	if filter.Since == nil {
		if filter.Limit == 0 {
			return nil, getPosition(stream), nil
		}
		items, _, err := stream.Get(0, false, filter.Limit, filter.Reverse)
		if err != nil {
			return nil, StreamPosition{}, err
		}
		pubs := make([]*Publication, 0, len(items))
		for _, item := range items {
			pub := item.Value.(*Publication)
			pubs = append(pubs, pub)
		}
		return pubs, getPosition(stream), nil
	}

	since := filter.Since

	streamPosition := getPosition(stream)

	if !filter.Reverse {
		if streamPosition.Offset == since.Offset && since.Epoch == stream.Epoch() {
			return nil, streamPosition, nil
		}
	}

	streamOffset := since.Offset + 1
	if filter.Reverse {
		streamOffset = since.Offset - 1
	}

	items, _, err := stream.Get(streamOffset, true, filter.Limit, filter.Reverse)
	if err != nil {
		return nil, StreamPosition{}, err
	}

	pubs := make([]*Publication, 0, len(items))
	for _, item := range items {
		pub := item.Value.(*Publication)
		pubs = append(pubs, pub)
	}
	return pubs, streamPosition, nil
}

func (h *historyHub) remove(ch string) error {
	h.Lock()
	defer h.Unlock()
	if stream, ok := h.streams[ch]; ok {
		stream.Clear()
	}
	return nil
}
