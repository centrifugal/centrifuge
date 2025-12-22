package centrifuge

import (
	"math/bits"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"
	"github.com/centrifugal/centrifuge/internal/timers"
)

type writerConfig struct {
	WriteManyFn  func(...queue.Item) error
	WriteFn      func(item queue.Item) error
	MaxQueueSize int
}

// writer helps to manage per-connection message byte queue.
type writer struct {
	mu       sync.Mutex
	config   writerConfig
	messages *queue.Queue
	closed   bool
	closeCh  chan struct{}
}

func newWriter(config writerConfig, queueInitialCap int) *writer {
	if queueInitialCap == 0 {
		queueInitialCap = 2
	}
	w := &writer{
		config:   config,
		messages: queue.New(queueInitialCap),
		closeCh:  make(chan struct{}),
	}
	return w
}

const (
	defaultMaxMessagesInFrame = 16
	maxItemBufLength          = 4096 // 2^12
)

// ItemBuf wraps []Item to avoid allocations when using sync.Pool
type ItemBuf struct {
	B []queue.Item
}

// pools contain pools for item slices of various capacities (power of 2)
var itemBufPools [13]sync.Pool // supports up to 2^12 = 4096

// nextLogBase2 returns log2(v) rounded up
func nextLogBase2(v uint32) uint32 {
	if v == 0 {
		return 0
	}
	return uint32(32 - bits.LeadingZeros32(v-1))
}

// prevLogBase2 returns log2(v) rounded down
func prevLogBase2(v uint32) uint32 {
	if v == 0 {
		return 0
	}
	next := nextLogBase2(v)
	if v == (1 << next) {
		return next
	}
	return next - 1
}

// getItemBuf returns an ItemBuf with capacity >= length
func getItemBuf(length int) *ItemBuf {
	if length <= 0 {
		length = defaultMaxMessagesInFrame
	}
	if length > maxItemBufLength {
		return &ItemBuf{
			B: make([]queue.Item, length),
		}
	}
	idx := nextLogBase2(uint32(length))
	if v := itemBufPools[idx].Get(); v != nil {
		buf := v.(*ItemBuf)
		buf.B = buf.B[:length]
		return buf
	}
	capacity := 1 << idx
	return &ItemBuf{
		B: make([]queue.Item, length, capacity),
	}
}

// putItemBuf returns buf to the pool
func putItemBuf(buf *ItemBuf) {
	capacity := cap(buf.B)
	if capacity == 0 || capacity > maxItemBufLength {
		return // drop oversized buffers
	}
	idx := prevLogBase2(uint32(capacity))
	// Clear the buffer
	for i := range buf.B {
		buf.B[i] = queue.Item{}
	}
	buf.B = buf.B[:0]
	itemBufPools[idx].Put(buf)
}

func (w *writer) waitSendMessage(maxMessagesInFrame int, writeDelay time.Duration) bool {
	// Wait for message from the queue.
	if !w.messages.Wait() {
		return false
	}

	var shrinkDelay time.Duration
	if writeDelay > 0 {
		w.messages.BeginCollect()
		shrinkDelay = time.Duration(float64(writeDelay) * 1.5)
		tm := timers.AcquireTimer(writeDelay)
		select {
		case <-tm.C:
		case <-w.closeCh:
			timers.ReleaseTimer(tm)
			w.messages.FinishCollect(shrinkDelay)
			return false
		}
		timers.ReleaseTimer(tm)
	}

	w.mu.Lock()

	// Determine buffer size
	bufSize := maxMessagesInFrame
	if bufSize == -1 {
		bufSize = w.messages.Len()
		if bufSize == 0 {
			bufSize = defaultMaxMessagesInFrame
		}
	}

	// Get buffer from tiered pool
	itemBuf := getItemBuf(bufSize)
	buf := itemBuf.B

	n, ok := w.messages.RemoveManyInto(buf, maxMessagesInFrame)
	if !ok {
		putItemBuf(itemBuf)
		w.mu.Unlock()
		if writeDelay > 0 {
			w.messages.FinishCollect(shrinkDelay)
		}
		return !w.messages.Closed()
	}

	items := buf[:n]
	var writeErr error
	if n == 1 {
		writeErr = w.config.WriteFn(items[0])
	} else {
		writeErr = w.config.WriteManyFn(items...)
	}

	putItemBuf(itemBuf)
	w.mu.Unlock()

	if writeDelay > 0 {
		w.messages.FinishCollect(shrinkDelay)
	}

	if writeErr != nil {
		// Write failed, transport must close itself, here we just return from routine.
		return false
	}
	return true
}

// run supposed to be run in goroutine, this goroutine will be closed as
// soon as queue is closed.
func (w *writer) run(writeDelay time.Duration, maxMessagesInFrame int) {
	if maxMessagesInFrame == 0 {
		maxMessagesInFrame = defaultMaxMessagesInFrame
	}
	for {
		if ok := w.waitSendMessage(maxMessagesInFrame, writeDelay); !ok {
			return
		}
	}
}

func (w *writer) enqueue(item queue.Item) *Disconnect {
	ok := w.messages.Add(item)
	if !ok {
		return &DisconnectConnectionClosed
	}
	if w.config.MaxQueueSize > 0 && w.messages.Size() > w.config.MaxQueueSize {
		return &DisconnectSlow
	}
	return nil
}

func (w *writer) enqueueMany(item ...queue.Item) *Disconnect {
	ok := w.messages.AddMany(item...)
	if !ok {
		return &DisconnectConnectionClosed
	}
	if w.config.MaxQueueSize > 0 && w.messages.Size() > w.config.MaxQueueSize {
		return &DisconnectSlow
	}
	return nil
}

func (w *writer) close(flushRemaining bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true

	if flushRemaining {
		remaining := w.messages.CloseRemaining()
		if len(remaining) > 0 {
			_ = w.config.WriteManyFn(remaining...)
		}
	} else {
		w.messages.Close()
	}
	close(w.closeCh)
	return nil
}
