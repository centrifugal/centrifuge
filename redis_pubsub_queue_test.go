package centrifuge

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func testPubSubMessage(channel string, size int) rueidis.PubSubMessage {
	return rueidis.PubSubMessage{Channel: channel, Message: string(make([]byte, size))}
}

func TestPubSubQueueDropsOverBudget(t *testing.T) {
	t.Parallel()
	budget := newPubSubBudget(100)
	q := newPubSubQueue(budget)
	defer q.close()
	require.True(t, q.add(testPubSubMessage("ch", 48))) // 50 bytes
	require.True(t, q.add(testPubSubMessage("ch", 48))) // 100
	require.False(t, q.add(testPubSubMessage("ch", 0))) // over by 2
	require.EqualValues(t, 100, budget.used.Load(), "a dropped message takes nothing")
	require.Equal(t, 2, q.len())
}

func TestPubSubQueueHandlesInOrderAndReleases(t *testing.T) {
	t.Parallel()
	budget := newPubSubBudget(1 << 20)
	q := newPubSubQueue(budget)
	var got []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		q.run(func(channel string, data []byte) {
			got = append(got, string(data))
		})
	}()
	for i := range 100 {
		require.True(t, q.add(rueidis.PubSubMessage{Channel: "ch", Message: strconv.Itoa(i)}))
	}
	require.Eventually(t, func() bool { return budget.used.Load() == 0 }, 5*time.Second, time.Millisecond)
	q.close()
	<-done
	require.Len(t, got, 100)
	for i, data := range got {
		require.Equal(t, strconv.Itoa(i), data)
	}
}

// add runs on the connection's read loop: a processor stuck in a handler must
// not make it wait, only make it drop once the budget is used up.
func TestPubSubQueueAddDoesNotWaitForStuckHandler(t *testing.T) {
	t.Parallel()
	budget := newPubSubBudget(10 * 1024)
	q := newPubSubQueue(budget)
	release := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		q.run(func(string, []byte) { <-release })
	}()

	added := make(chan int)
	go func() {
		n := 0
		for range 100000 {
			if q.add(testPubSubMessage("ch", 1022)) {
				n++
			}
		}
		added <- n
	}()
	select {
	case n := <-added:
		require.Equal(t, 10, n, "the budget, plus none: the stuck one is still counted")
	case <-time.After(5 * time.Second):
		t.Fatal("add waited for the processor")
	}
	close(release)
	require.Eventually(t, func() bool { return budget.used.Load() == 0 }, 5*time.Second, time.Millisecond)
	q.close()
	<-stopped
}

func TestPubSubQueueCloseStopsRunAndReleases(t *testing.T) {
	t.Parallel()
	budget := newPubSubBudget(1 << 20)

	// Idle processor: close must wake it.
	idle := newPubSubQueue(budget)
	idleStopped := make(chan struct{})
	go func() {
		defer close(idleStopped)
		idle.run(func(string, []byte) {})
	}()
	time.Sleep(10 * time.Millisecond)
	idle.close()
	select {
	case <-idleStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not stop an idle processor")
	}

	// Busy processor: what waits is released, what it handles is released by it.
	busy := newPubSubQueue(budget)
	entered := make(chan struct{})
	release := make(chan struct{})
	busyStopped := make(chan struct{})
	go func() {
		defer close(busyStopped)
		busy.run(func(string, []byte) {
			close(entered)
			<-release
		})
	}()
	for range 5 {
		require.True(t, busy.add(testPubSubMessage("ch", 98)))
	}
	<-entered
	busy.close()
	require.EqualValues(t, 100, budget.used.Load(), "only the message being handled is still counted")
	require.False(t, busy.add(testPubSubMessage("ch", 98)), "a closed queue takes nothing")
	require.EqualValues(t, 100, budget.used.Load())
	close(release)
	select {
	case <-busyStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not stop a busy processor")
	}
	require.Zero(t, budget.used.Load())
}

// Connections of a broker share its budget: queues added to, handled and
// closed concurrently must return every byte.
func TestPubSubBudgetSharedNoLeak(t *testing.T) {
	t.Parallel()
	budget := newPubSubBudget(64 * 1024)
	for round := range 20 {
		queues := make([]*pubSubQueue, 4)
		var running sync.WaitGroup
		for i := range queues {
			queues[i] = newPubSubQueue(budget)
			running.Go(func() {
				queues[i].run(func(string, []byte) {
					if round%2 == 0 {
						time.Sleep(time.Microsecond)
					}
				})
			})
		}
		var adding sync.WaitGroup
		for w := range 8 {
			adding.Go(func() {
				for i := range 2000 {
					queues[(w+i)%len(queues)].add(testPubSubMessage("ch", 100+i%300))
				}
			})
		}
		// Close while messages still arrive: late adds must not leak either.
		time.Sleep(time.Millisecond)
		for _, q := range queues {
			q.close()
		}
		adding.Wait()
		running.Wait()
		require.Zero(t, budget.used.Load(), "round %d", round)
	}
}

func TestPubSubBudgetDefault(t *testing.T) {
	t.Parallel()
	require.EqualValues(t, defaultPubSubQueueMaxSize, newPubSubBudget(0).limit)
	require.EqualValues(t, defaultPubSubQueueMaxSize, newPubSubBudget(-1).limit)
	require.EqualValues(t, 1024, newPubSubBudget(1024).limit)
}
