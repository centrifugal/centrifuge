package centrifuge

import (
	"sync/atomic"

	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/centrifugal/centrifuge/internal/queue"
	"github.com/redis/rueidis"
)

// defaultPubSubQueueMaxSize is the default of PubSubQueueMaxSize.
const defaultPubSubQueueMaxSize = 64 << 20

// pubSubBudget bounds the bytes of PUB/SUB messages which a broker holds while
// they wait to be processed, over all its connections and processors.
type pubSubBudget struct {
	limit int64
	used  atomic.Int64
}

func newPubSubBudget(maxSize int) *pubSubBudget {
	if maxSize <= 0 {
		maxSize = defaultPubSubQueueMaxSize
	}
	return &pubSubBudget{limit: int64(maxSize)}
}

func pubSubItemSize(channel string, data []byte) int64 {
	return int64(len(channel) + len(data))
}

// pubSubQueue holds the messages of one processor. It takes memory only while
// messages wait.
type pubSubQueue struct {
	q      *queue.Queue
	budget *pubSubBudget
}

func newPubSubQueue(budget *pubSubBudget) *pubSubQueue {
	return &pubSubQueue{q: queue.New(2), budget: budget}
}

// add queues a message, or reports false when it is dropped: the budget is
// used up or the queue is closed. It is called by the connection's read loop,
// so it never waits for a processor - blocking there would stop reading from
// Redis (see https://github.com/redis/rueidis/issues/596).
func (p *pubSubQueue) add(msg rueidis.PubSubMessage) bool {
	data := convert.StringToBytes(msg.Message)
	size := pubSubItemSize(msg.Channel, data)
	if p.budget.used.Add(size) > p.budget.limit {
		p.budget.used.Add(-size)
		return false
	}
	if !p.q.Add(queue.Item{Channel: msg.Channel, Data: data}) {
		p.budget.used.Add(-size)
		return false
	}
	return true
}

// run handles messages in the order they were added until the queue is closed.
// No lock is held while handle runs.
func (p *pubSubQueue) run(handle func(channel string, data []byte)) {
	for p.q.Wait() {
		item, ok := p.q.Remove()
		if !ok {
			continue
		}
		handle(item.Channel, item.Data)
		p.budget.used.Add(-pubSubItemSize(item.Channel, item.Data))
	}
}

// close stops run and drops the messages still waiting, returning their bytes
// to the budget: it is shared by the broker's connections, and what a closed
// connection kept would otherwise be lost to the others for good.
func (p *pubSubQueue) close() {
	for _, item := range p.q.CloseRemaining() {
		p.budget.used.Add(-pubSubItemSize(item.Channel, item.Data))
	}
}

func (p *pubSubQueue) len() int {
	return p.q.Len()
}
