package centrifuge

import (
	"github.com/centrifugal/centrifuge/internal/queue"
	"github.com/centrifugal/centrifuge/internal/timers"
	"sync"
	"time"
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

func newWriter(config writerConfig) *writer {
	w := &writer{
		config:   config,
		messages: queue.New(),
		closeCh:  make(chan struct{}),
	}
	return w
}

const (
	defaultMaxMessagesInFrame = 16
)

func (w *writer) waitSendMessage(maxMessagesInFrame int, batchDelay time.Duration) bool {
	// Wait for message from queue.
	ok := w.messages.Wait()
	if !ok {
		return false
	}

	if batchDelay > 0 {
		tm := timers.AcquireTimer(batchDelay)
		if batchDelay > 0 {
			select {
			case <-tm.C:
			case <-w.closeCh:
			}
		}
		timers.ReleaseTimer(tm)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	msg, ok := w.messages.Remove()
	if !ok {
		return !w.messages.Closed()
	}

	var writeErr error

	messageCount := w.messages.Len()
	if maxMessagesInFrame > 1 && messageCount > 0 {
		// There are several more messages left in queue, try to send them in single frame,
		// but no more than maxMessagesInFrame.

		// Limit message count to get from queue with (maxMessagesInFrame - 1)
		// (as we already have one message received from queue above).
		messagesCap := messageCount + 1
		if messagesCap > maxMessagesInFrame {
			messagesCap = maxMessagesInFrame
		}

		messages := make([]queue.Item, 0, messagesCap)
		messages = append(messages, msg)

		for messageCount > 0 {
			messageCount--
			if len(messages) >= maxMessagesInFrame {
				break
			}
			m, ok := w.messages.Remove()
			if ok {
				messages = append(messages, m)
			} else {
				if w.messages.Closed() {
					return false
				}
				break
			}
		}
		if len(messages) == 1 {
			writeErr = w.config.WriteFn(messages[0])
		} else {
			writeErr = w.config.WriteManyFn(messages...)
		}
	} else {
		// WriteMany single message without allocating new slice.
		writeErr = w.config.WriteFn(msg)
	}
	if writeErr != nil {
		// WriteMany failed, transport must close itself, here we just return from routine.
		return false
	}
	return true
}

// run supposed to be run in goroutine, this goroutine will be closed as
// soon as queue is closed.
func (w *writer) run(batchDelay time.Duration, maxMessagesInFrame int) {
	if maxMessagesInFrame == 0 {
		maxMessagesInFrame = defaultMaxMessagesInFrame
	}
	for {
		if ok := w.waitSendMessage(maxMessagesInFrame, batchDelay); !ok {
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
			// TODO: make it respect MaxMessagesInFrame option.
			_ = w.config.WriteManyFn(remaining...)
		}
	} else {
		w.messages.Close()
	}
	close(w.closeCh)
	return nil
}
