package recovery

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// testSync queues the write of a publication as a func.
type testSync = PubSubSync[func()]

func call(write func()) { write() }

// read returns the publications buffered till the sync point.
func read(s *testSync, b *Buffer[func()]) []*protocol.Publication {
	pubs, _ := s.ReadBuffered(b, "")
	return pubs
}

// recorder is what a client writes, in order.
type recorder struct {
	mu      sync.Mutex
	offsets []uint64
}

func (r *recorder) write(offset uint64) func() {
	return func() {
		r.mu.Lock()
		r.offsets = append(r.offsets, offset)
		r.mu.Unlock()
	}
}

func (r *recorder) get() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.offsets...)
}

// publish is what the broadcast path does: sync the publication, or write it.
func (r *recorder) publish(s *testSync, channel string, offset uint64, size int) {
	pub := &protocol.Publication{Offset: offset}
	if s.Buffering() && s.SyncPublication(channel, pub, "", size, r.write(offset)) {
		return
	}
	r.write(offset)()
}

func offsetsOf(pubs []*protocol.Publication) []uint64 {
	var offsets []uint64
	for _, pub := range pubs {
		offsets = append(offsets, pub.Offset)
	}
	return offsets
}

func TestPubSubSyncPhases(t *testing.T) {
	s := &testSync{}
	r := &recorder{}

	r.publish(s, "ch", 1, 1)
	require.Equal(t, []uint64{1}, r.get(), "publication must pass through when channel is not buffering")
	require.False(t, s.Buffering())

	b := s.StartBuffering("ch", 0)
	require.True(t, s.Buffering())
	r.publish(s, "ch", 2, 1)
	r.publish(s, "other", 100, 1)
	require.Equal(t, []uint64{1, 100}, r.get(), "only the buffering channel is synced")

	require.Equal(t, []uint64{2}, offsetsOf(read(s, b)))
	require.Nil(t, read(s, b), "a second sync point returns nothing")

	r.publish(s, "ch", 3, 1)
	r.publish(s, "ch", 4, 1)
	require.Equal(t, []uint64{1, 100}, r.get(), "publications after the sync point wait for StopBuffering")

	require.False(t, s.StopBuffering(b, call))
	require.Equal(t, []uint64{1, 100, 3, 4}, r.get())
	require.False(t, s.Buffering())
	require.Empty(t, s.buffers)

	r.publish(s, "ch", 5, 1)
	require.Equal(t, []uint64{1, 100, 3, 4, 5}, r.get(), "publication must pass through after buffering stopped")

	require.False(t, s.StopBuffering(b, call), "a second stop does nothing")
	s.CancelBuffering(b)
	require.Equal(t, []uint64{1, 100, 3, 4, 5}, r.get())
}

func TestPubSubSyncCancel(t *testing.T) {
	s := &testSync{}
	r := &recorder{}
	b := s.StartBuffering("ch", 0)
	r.publish(s, "ch", 1, 1)
	s.ReadBuffered(b, "")
	r.publish(s, "ch", 2, 1)
	s.CancelBuffering(b)
	require.Empty(t, r.get(), "cancel drops the queue")
	require.False(t, s.Buffering())
	require.False(t, s.StopBuffering(b, call), "stop after cancel does nothing")
	require.Empty(t, r.get())
	r.publish(s, "ch", 3, 1)
	require.Equal(t, []uint64{3}, r.get())
}

func TestPubSubSyncStopWithoutSyncPoint(t *testing.T) {
	// A subscription which reads no stream never gets to the sync point: what it
	// collected is dropped, as before.
	s := &testSync{}
	r := &recorder{}
	b := s.StartBuffering("ch", 0)
	r.publish(s, "ch", 1, 1)
	require.False(t, s.StopBuffering(b, call))
	require.Empty(t, r.get())
	r.publish(s, "ch", 2, 1)
	require.Equal(t, []uint64{2}, r.get())
}

func TestPubSubSyncOverflow(t *testing.T) {
	s := &testSync{}
	r := &recorder{}
	b := s.StartBuffering("ch", 10)
	s.ReadBuffered(b, "")
	r.publish(s, "ch", 1, 6)
	r.publish(s, "ch", 2, 6) // Over the limit: the queue is dropped.
	r.publish(s, "ch", 3, 1) // Dropped as well, it would leave a gap.
	require.True(t, s.StopBuffering(b, call))
	require.Empty(t, r.get())
	require.False(t, s.Buffering())
	r.publish(s, "ch", 4, 1)
	require.Equal(t, []uint64{4}, r.get())
}

func TestPubSubSyncNewerBufferForChannel(t *testing.T) {
	// A newer subscribe attempt to the channel replaces the buffer in the map;
	// stopping or cancelling the older one must not touch it.
	s := &testSync{}
	r := &recorder{}
	older := s.StartBuffering("ch", 0)
	newer := s.StartBuffering("ch", 0)
	s.CancelBuffering(older)
	require.True(t, s.Buffering())
	r.publish(s, "ch", 1, 1)
	require.Equal(t, []uint64{1}, offsetsOf(read(s, newer)))
	r.publish(s, "ch", 2, 1)
	require.False(t, s.StopBuffering(older, call))
	require.Empty(t, r.get())
	require.False(t, s.StopBuffering(newer, call))
	require.Equal(t, []uint64{2}, r.get())
	require.False(t, s.Buffering())
}

func TestPubSubSyncPublicationWhileFlushing(t *testing.T) {
	// A publication which comes while StopBuffering writes the queue (here from
	// inside a write, deterministically) is queued behind it, not written ahead.
	s := &testSync{}
	r := &recorder{}
	b := s.StartBuffering("ch", 0)
	s.ReadBuffered(b, "")
	s.SyncPublication("ch", &protocol.Publication{Offset: 1}, "", 1, func() {
		r.write(1)()
		r.publish(s, "ch", 3, 1)
	})
	r.publish(s, "ch", 2, 1)
	require.False(t, s.StopBuffering(b, call))
	require.Equal(t, []uint64{1, 2, 3}, r.get())
	r.publish(s, "ch", 4, 1)
	require.Equal(t, []uint64{1, 2, 3, 4}, r.get())
}

func TestPubSubSyncCancelWithConcurrentPublisher(t *testing.T) {
	// Nothing is written for a cancelled attempt, and publications after the
	// cancel pass through.
	for i := 0; i < 100; i++ {
		s := &testSync{}
		r := &recorder{}
		b := s.StartBuffering("ch", 0)
		s.ReadBuffered(b, "")
		done := make(chan struct{})
		go func() {
			defer close(done)
			for offset := uint64(1); offset <= 100; offset++ {
				r.publish(s, "ch", offset, 1)
			}
		}()
		s.CancelBuffering(b)
		<-done
		written := r.get()
		for j := 1; j < len(written); j++ {
			require.Equal(t, written[j-1]+1, written[j], "only publications after the cancel are written, in order")
		}
		if len(written) > 0 {
			require.Equal(t, uint64(100), written[len(written)-1])
		}
		require.False(t, s.Buffering())
	}
}

func TestPubSubSyncLongQueueOrder(t *testing.T) {
	// A queue long enough to span chunks of every size keeps its order, also with
	// a publication which comes while it is written.
	s := &testSync{}
	r := &recorder{}
	b := s.StartBuffering("ch", 0)
	s.ReadBuffered(b, "")
	const n = 300
	for offset := uint64(1); offset <= n; offset++ {
		if offset == 100 {
			s.SyncPublication("ch", &protocol.Publication{Offset: offset}, "", 1, func() {
				r.write(100)()
				r.publish(s, "ch", n+1, 1)
			})
			continue
		}
		r.publish(s, "ch", offset, 1)
	}
	require.Empty(t, r.get())
	require.False(t, s.StopBuffering(b, call))
	written := r.get()
	require.Len(t, written, n+1)
	for i, offset := range written {
		require.Equal(t, uint64(i+1), offset)
	}
}

func TestPubSubSyncLongQueueOverflow(t *testing.T) {
	// Overflow in the middle of a long queue drops all of it.
	s := &testSync{}
	r := &recorder{}
	b := s.StartBuffering("ch", 200)
	s.ReadBuffered(b, "")
	for offset := uint64(1); offset <= 300; offset++ {
		r.publish(s, "ch", offset, 1)
	}
	require.True(t, s.StopBuffering(b, call))
	require.Empty(t, r.get())
}

func TestPubSubSyncEpochAtSyncPoint(t *testing.T) {
	// Publications collected before the sync point can only be merged with history
	// of their own epoch.
	tests := []struct {
		name        string
		collected   []string // Epochs of the collected publications.
		historyFrom string
		ok          bool
	}{
		{name: "same", collected: []string{"a", "a"}, historyFrom: "a", ok: true},
		{name: "other", collected: []string{"b"}, historyFrom: "a", ok: false},
		{name: "changed_while_collecting", collected: []string{"a", "b"}, historyFrom: "a", ok: false},
		{name: "changed_back", collected: []string{"b", "a"}, historyFrom: "a", ok: false},
		{name: "unknown_history_epoch", collected: []string{"b", "b"}, historyFrom: "", ok: true},
		{name: "unknown_history_epoch_changed_while_collecting", collected: []string{"b", "c"}, historyFrom: "", ok: false},
		{name: "nothing_collected", historyFrom: "a", ok: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &testSync{}
			b := s.StartBuffering("ch", 0)
			for i, epoch := range tt.collected {
				require.True(t, s.SyncPublication("ch", &protocol.Publication{Offset: uint64(i + 1)}, epoch, 1, func() {}))
			}
			pubs, ok := s.ReadBuffered(b, tt.historyFrom)
			require.Len(t, pubs, len(tt.collected))
			require.Equal(t, tt.ok, ok)
			s.CancelBuffering(b)
		})
	}
}

func TestPubSubSyncNilBuffer(t *testing.T) {
	s := &testSync{}
	pubs, ok := s.ReadBuffered(nil, "epoch")
	require.Nil(t, pubs)
	require.True(t, ok)
	require.False(t, s.StopBuffering(nil, call))
	s.CancelBuffering(nil)
}

// A publisher runs through a subscribe cycle: every publication broadcast to the
// subscription must end up either in the buffered ones (merged into the subscribe
// result) or written, exactly once, in order, and nothing is written before
// StopBuffering. As in the hub, a publication reaches the subscription only once it
// is added, under a lock, after StartBuffering.
func TestPubSubSyncOrderUnderConcurrentPublisher(t *testing.T) {
	for i := 0; i < 200; i++ {
		s := &testSync{}
		r := &recorder{}
		var hubMu sync.RWMutex
		subscribed := false
		var firstDelivered uint64
		var delivered atomic.Int64
		stop := make(chan struct{})
		published := make(chan uint64)
		go func() {
			var offset uint64
			for {
				select {
				case <-stop:
					published <- offset
					return
				default:
				}
				offset++
				hubMu.RLock()
				if subscribed {
					if firstDelivered == 0 {
						firstDelivered = offset
					}
					r.publish(s, "ch", offset, 1)
					delivered.Add(1)
				}
				hubMu.RUnlock()
				runtime.Gosched()
			}
		}()
		// Each phase gets publications, however the goroutines are scheduled.
		waitDelivered := func(n int64) {
			for delivered.Load() < n {
				runtime.Gosched()
			}
		}
		b := s.StartBuffering("ch", 0)
		hubMu.Lock()
		subscribed = true
		hubMu.Unlock()
		waitDelivered(3)
		buffered := offsetsOf(read(s, b))
		waitDelivered(delivered.Load() + 3)
		require.Empty(t, r.get(), "nothing is written before StopBuffering")
		s.StopBuffering(b, call)
		waitDelivered(delivered.Load() + 3)
		close(stop)
		last := <-published

		all := append(buffered, r.get()...)
		require.NotEmpty(t, buffered, "iteration %d", i)
		require.Equal(t, firstDelivered, all[0], "iteration %d", i)
		require.Len(t, all, int(last-firstDelivered+1), "iteration %d", i)
		for j, offset := range all {
			require.Equal(t, firstDelivered+uint64(j), offset, "iteration %d", i)
		}
	}
}

// A client which is not subscribing: the broadcast path's only cost is Buffering.
func BenchmarkPubSubSyncNotBuffering(b *testing.B) {
	s := &testSync{}
	pub := &protocol.Publication{Offset: 1}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if s.Buffering() && s.SyncPublication("ch", pub, "", 1, func() {}) {
				b.Fatal("synced")
			}
		}
	})
}

// A client subscribing to other channels than the one broadcast to.
func BenchmarkPubSubSyncOtherChannelBuffering(b *testing.B) {
	s := &testSync{}
	s.StartBuffering("other", 0)
	pub := &protocol.Publication{Offset: 1}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if s.Buffering() && s.SyncPublication("ch", pub, "", 1, func() {}) {
				b.Fatal("synced")
			}
		}
	})
}

// benchItem is as big as what the client queues for a publication.
type benchItem [26]uint64

// A subscribe cycle, with publications queued between the sync point and
// StopBuffering.
func BenchmarkPubSubSyncSubscribeCycle(b *testing.B) {
	pub := &protocol.Publication{Offset: 1}
	for _, queued := range []int{0, 10, 100} {
		b.Run("queued_"+strconv.Itoa(queued), func(b *testing.B) {
			s := &PubSubSync[benchItem]{}
			var written int
			write := func(benchItem) { written++ }
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf := s.StartBuffering("ch", 0)
				s.SyncPublication("ch", pub, "", 1, benchItem{})
				_, _ = s.ReadBuffered(buf, "")
				for j := 0; j < queued; j++ {
					s.SyncPublication("ch", pub, "", 1, benchItem{})
				}
				s.StopBuffering(buf, write)
			}
			if written != b.N*queued {
				b.Fatalf("written %d, want %d", written, b.N*queued)
			}
		})
	}
}
