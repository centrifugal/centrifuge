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

	// Timer-driven mode fields (when writeDelay > 0 and useWriteTimer is true).
	timerMode          bool
	writeDelay         time.Duration
	maxMessagesInFrame int
	shrinkDelay        time.Duration
	flushTimer         *time.Timer
	timerScheduled     bool
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
)

func (w *writer) waitSendMessage(maxMessagesInFrame int, writeDelay time.Duration, shrinkDelay time.Duration) bool {
	// Wait for message from the queue.
	if !w.messages.Wait() {
		return false
	}

	if writeDelay > 0 {
		if maxMessagesInFrame == -1 || w.messages.Len() < maxMessagesInFrame {
			// Only wait if we have not enough messages to fill the frame.
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
		bufSize := maxMessagesInFrame
		if bufSize < 0 { // Unlimited, just use current length.
			bufSize = w.messages.Len()
			if bufSize == 0 {
				return true
			}
		}

		// Get buffer from tiered pool.
		itemBuf := getItemBuf(bufSize)
		buf := itemBuf.B

		n, ok := w.messages.RemoveManyInto(buf, bufSize)
		if !ok {
			putItemBuf(itemBuf)
			w.mu.Unlock()
			w.messages.FinishCollect(shrinkDelay)
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
		w.messages.FinishCollect(shrinkDelay)

		if writeErr != nil {
			// Write failed, transport must close itself, here we just return from routine.
			return false
		}
		return true
	}

	// No batching - use RemoveMany which handles shrinking automatically.
	w.mu.Lock()
	defer w.mu.Unlock()

	items, ok := w.messages.RemoveMany(maxMessagesInFrame)
	if !ok {
		return !w.messages.Closed()
	}

	var writeErr error
	if len(items) == 1 {
		writeErr = w.config.WriteFn(items[0])
	} else {
		writeErr = w.config.WriteManyFn(items...)
	}

	if writeErr != nil {
		// Write failed, transport must close itself, here we just return from routine.
		return false
	}
	return true
}

// run supposed to be run in goroutine, this goroutine will be closed as
// soon as queue is closed. When writeDelay > 0, this method is non-blocking
// and uses a timer-driven approach instead of a dedicated goroutine.
func (w *writer) run(writeDelay time.Duration, maxMessagesInFrame int, shrinkDelay time.Duration, useWriteTimer bool) {
	if maxMessagesInFrame == 0 {
		maxMessagesInFrame = defaultMaxMessagesInFrame
	}

	// Timer-driven mode for writeDelay > 0 and useWriteTimer: non-blocking, triggered by enqueue.
	if writeDelay > 0 && useWriteTimer {
		w.mu.Lock()
		w.timerMode = true
		w.writeDelay = writeDelay
		w.maxMessagesInFrame = maxMessagesInFrame
		w.shrinkDelay = shrinkDelay
		w.mu.Unlock()
		return
	}

	// Traditional dedicated goroutine mode.
	for {
		if ok := w.waitSendMessage(maxMessagesInFrame, writeDelay, shrinkDelay); !ok {
			return
		}
	}
}

// flush is called by the timer in timer-driven mode to batch and write messages
func (w *writer) flush() {
	w.mu.Lock()

	w.timerScheduled = false

	// Check if there are messages to flush
	messagesLen := w.messages.Len()
	if messagesLen == 0 {
		w.mu.Unlock()
		return
	}

	// Determine buffer size
	bufSize := w.maxMessagesInFrame
	if bufSize < 0 { // Unlimited, just use current length.
		bufSize = messagesLen
	}

	// Get buffer from tiered pool
	itemBuf := getItemBuf(bufSize)
	buf := itemBuf.B

	n, ok := w.messages.RemoveManyInto(buf, bufSize)
	if !ok {
		putItemBuf(itemBuf)
		w.mu.Unlock()
		w.messages.FinishCollect(w.shrinkDelay)
		return
	}

	items := buf[:n]
	var writeErr error
	if n == 1 {
		writeErr = w.config.WriteFn(items[0])
	} else {
		writeErr = w.config.WriteManyFn(items...)
	}

	putItemBuf(itemBuf)

	// If there are still messages and no error, schedule another flush
	if writeErr == nil && w.messages.Len() > 0 && !w.closed {
		remainingMessages := w.messages.Len()
		// If we have more messages than max batch size (and max is not unlimited),
		// flush immediately to keep up, otherwise we'll fall behind.
		if w.maxMessagesInFrame > 0 && remainingMessages >= w.maxMessagesInFrame {
			w.scheduleFlushImmediateLocked()
		} else {
			// Messages below threshold or unlimited batch size, use normal delay
			w.scheduleFlushLocked()
		}
	}

	w.mu.Unlock()
	w.messages.FinishCollect(w.shrinkDelay)
}

// scheduleFlushLocked schedules a flush timer with normal delay. Must be called with w.mu held.
func (w *writer) scheduleFlushLocked() {
	if w.timerScheduled {
		return
	}
	w.timerScheduled = true
	if w.flushTimer == nil {
		w.flushTimer = time.AfterFunc(w.writeDelay, w.flush)
	} else {
		w.flushTimer.Reset(w.writeDelay)
	}
}

// scheduleFlushImmediateLocked schedules an immediate flush (0 delay). Must be called with w.mu held.
func (w *writer) scheduleFlushImmediateLocked() {
	if w.timerScheduled {
		return
	}
	w.timerScheduled = true
	if w.flushTimer == nil {
		w.flushTimer = time.AfterFunc(0, w.flush)
	} else {
		w.flushTimer.Reset(0)
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

	// In timer mode, schedule flush if not already scheduled
	if w.timerMode {
		w.mu.Lock()
		if !w.closed && !w.timerScheduled {
			w.scheduleFlushLocked()
		}
		w.mu.Unlock()
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

	// In timer mode, schedule flush if not already scheduled
	if w.timerMode {
		w.mu.Lock()
		if !w.closed && !w.timerScheduled {
			w.scheduleFlushLocked()
		}
		w.mu.Unlock()
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

	// Stop flush timer if running
	if w.flushTimer != nil {
		w.flushTimer.Stop()
	}

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

const (
	maxItemBufLength = 4096 // 2^12
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
