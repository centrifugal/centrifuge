package centrifuge

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestMemoryBroker_PublishHistoryReadRace guards a data race on
// Publication.Offset. historyHub.add already stamps pub.Offset under the history
// lock and returns that offset, but Publish then set pub.Offset a second time
// outside the lock — a redundant, unsynchronized write that raced a concurrent
// History read of the same shared *Publication (e.g. subscribe-time recovery
// reading Publications[i].Offset). Must be run under -race.
func TestMemoryBroker_PublishHistoryReadRace(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	const ch = "hist-race"

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = node.Publish(ch, []byte(`{"x":1}`), WithHistory(10, time.Minute))
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := node.History(ch, WithLimit(10))
				if err != nil {
					continue
				}
				for _, p := range res.Publications {
					_ = p.Offset
				}
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}
