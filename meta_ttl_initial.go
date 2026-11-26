package centrifuge

import (
	"container/heap"
	"fmt"
	"math"
	"time"

	"github.com/centrifugal/centrifuge/internal/priority"
)

// initialMetaTTLFor is the initial TTL of a channel: its own (from
// SubscribeOptions, HistoryOptions, HistoryReply or MapChannelOptions), or
// global (Config.HistoryMetaTTLInitial) when its own is zero. A negative one
// turns it off: own for the channel, global for all channels without their
// own. Zero: off.
func initialMetaTTLFor(own, global time.Duration) time.Duration {
	if own < 0 {
		return 0
	}
	if own > 0 {
		return own
	}
	return max(global, 0)
}

// checkMetaTTLInitial checks an initial TTL (name says where it is set) against
// the position check delay: live subscribers extend a meta without
// publications only by their position checks, so a TTL shorter than 3 of them
// could expire the meta under them, which starts a new epoch. Zero or
// negative: nothing to check.
func checkMetaTTLInitial(name string, ttl time.Duration, c Config) error {
	if ttl > 0 && ttl < 3*c.ClientChannelPositionCheckDelay {
		return fmt.Errorf("%s (%s) must be at least 3 × ClientChannelPositionCheckDelay (%s)", name, ttl, c.ClientChannelPositionCheckDelay)
	}
	return nil
}

// emptyMetaTTL is the TTL of a meta without publications with
// Config.HistoryMetaTTLInitial set: the initial TTL, or metaTTL if that is
// shorter. A zero metaTTL means the meta doesn't expire otherwise.
func emptyMetaTTL(metaTTL, initial time.Duration) time.Duration {
	if metaTTL == 0 || initial < metaTTL {
		return initial
	}
	return metaTTL
}

// scheduleRemove has the memory brokers remove ch at removeAt. Unlike a plain
// update of removes it also queues a removal earlier than the one queued, as
// the initial TTL can be shorter than the TTL a meta had.
func scheduleRemove(queue *priority.Queue, removes map[string]int64, nextCheck *int64, ch string, removeAt int64) {
	if prev, ok := removes[ch]; !ok || removeAt < prev {
		heap.Push(queue, &priority.Item{Value: ch, Priority: removeAt})
	}
	removes[ch] = removeAt
	if *nextCheck == 0 || *nextCheck > removeAt {
		*nextCheck = removeAt
	}
}

// metaTTLInitialSeconds is how ChannelContext keeps an initial TTL: rounded up
// to seconds, so that a sub-second one doesn't become 0 (the global one), and
// -1 for a negative one (off).
func metaTTLInitialSeconds(ttl time.Duration) int32 {
	if ttl < 0 {
		return -1
	}
	return int32(min(math.Ceil(ttl.Seconds()), math.MaxInt32))
}
