package main

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"

	cent "github.com/centrifugal/centrifuge-go"
)

// ---------------------------------------------------------------------------
// Goroutine leak detection.
//
// Counting runtime.NumGoroutine() would be useless here: the harness, the SDK
// and net/http keep their own goroutines around and the number moves for
// reasons that have nothing to do with the library. So instead the suite counts
// only goroutines that are *inside centrifuge library code* and compares that
// against a baseline taken after a warm-up — a connection, a subscription, a
// publication and a clean close on every node. Anything the library starts
// lazily on first use is therefore already in the baseline, and after the suite
// drains the count has to come back to exactly that number.
//
// This is the check that catches a per-connection reader/writer/timer goroutine
// that outlives its client: connect_churn alone opens hundreds of thousands of
// connections, so even a rare leak shows up as a large number here.
// ---------------------------------------------------------------------------

const (
	// Library frames look like "github.com/centrifugal/centrifuge.(*Client).…"
	// or "github.com/centrifugal/centrifuge/internal/…". Neither prefix matches
	// "github.com/centrifugal/centrifuge-go." (the SDK) or the harness itself,
	// whose functions are in package main.
	libFramePrefix     = "github.com/centrifugal/centrifuge."
	libInternalPrefix  = "github.com/centrifugal/centrifuge/internal/"
	goroutineStackSize = 4 << 20
)

// goroutineStacks returns one string per live goroutine.
func goroutineStacks() []string {
	buf := make([]byte, goroutineStackSize)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Split(strings.TrimSpace(string(buf[:n])), "\n\n")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// libGoroutines counts goroutines executing centrifuge library code, keyed by
// the deepest library frame they are in — which is what identifies the leak.
func libGoroutines() (int, map[string]int) {
	counts := map[string]int{}
	total := 0
	for _, stack := range goroutineStacks() {
		frame, ok := topLibFrame(stack)
		if !ok {
			continue
		}
		total++
		counts[frame]++
	}
	return total, counts
}

// topLibFrame returns the innermost centrifuge library function on the stack.
func topLibFrame(stack string) (string, bool) {
	for _, line := range strings.Split(stack, "\n") {
		if !strings.HasPrefix(line, libFramePrefix) && !strings.HasPrefix(line, libInternalPrefix) {
			continue
		}
		// Trim the argument list: "pkg.(*T).fn(0x14000…, 0x1)" -> "pkg.(*T).fn".
		if i := strings.LastIndex(line, "("); i > 0 {
			line = line[:i]
		}
		return strings.TrimSpace(line), true
	}
	return "", false
}

// warmup exercises every node once so anything the library starts lazily is
// running before the baseline is taken, then waits for it all to drain again.
func warmup(e *env) error {
	type target struct {
		name  string
		wsURL string
	}
	targets := []target{{"main", e.wsURL}, {"strict", e.strictWSURL}}
	if e.redisEnabled() {
		targets = append(targets, target{"redis-a", e.redisAWS}, target{"redis-b", e.redisBWS})
	}
	for _, t := range targets {
		co, err := dial(t.wsURL, newUser("warmup"))
		if err != nil {
			return fmt.Errorf("%s: dial: %w", t.name, err)
		}
		ch := newChannel(chRecov, "warmup")
		sub, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true}, nil)
		if err != nil {
			co.c.Close()
			return fmt.Errorf("%s: subscribe: %w", t.name, err)
		}
		wctx, cancel := newTimeoutContext(10 * time.Second)
		_, err = sub.Publish(wctx, []byte(`{"warmup":1}`))
		cancel()
		if err != nil {
			co.c.Close()
			return fmt.Errorf("%s: publish: %w", t.name, err)
		}
		// Presence and history too — each has its own lazily started machinery.
		wctx, cancel = newTimeoutContext(10 * time.Second)
		_, _ = sub.Presence(wctx)
		_, _ = sub.History(wctx, cent.WithHistoryLimit(1))
		cancel()
		co.c.Close()
	}
	// Let the connections finish tearing down before the baseline is taken.
	for name, n := range e.nodes() {
		if !waitFor(15*time.Second, func() bool { return n.Hub().NumClients() == 0 }) {
			return fmt.Errorf("%s: hub did not drain after warm-up", name)
		}
	}
	time.Sleep(500 * time.Millisecond)
	return nil
}

// goroutineLeakCheck compares the current library goroutine count against the
// post-warm-up baseline.
func goroutineLeakCheck(baseline int, baselineFrames map[string]int) result {
	var got int
	var frames map[string]int
	ok := waitFor(30*time.Second, func() bool {
		got, frames = libGoroutines()
		return got <= baseline
	})
	if ok {
		return result{name: "no_goroutine_leaks", pass: true,
			detail: fmt.Sprintf("library goroutines back to the %d-goroutine baseline (now %d)", baseline, got)}
	}
	return result{name: "no_goroutine_leaks",
		detail: fmt.Sprintf("%d library goroutines still running, baseline is %d — %s",
			got, baseline, describeExtraFrames(baselineFrames, frames))}
}

// describeExtraFrames names the frames that grew relative to the baseline,
// biggest first — that list is the leak.
func describeExtraFrames(baseline, now map[string]int) string {
	type delta struct {
		frame string
		n     int
	}
	var deltas []delta
	for frame, n := range now {
		if d := n - baseline[frame]; d > 0 {
			deltas = append(deltas, delta{frame, d})
		}
	}
	if len(deltas) == 0 {
		return "no single frame grew (goroutines moved between frames)"
	}
	sort.Slice(deltas, func(i, j int) bool {
		if deltas[i].n != deltas[j].n {
			return deltas[i].n > deltas[j].n
		}
		return deltas[i].frame < deltas[j].frame
	})
	if len(deltas) > 5 {
		deltas = deltas[:5]
	}
	parts := make([]string, 0, len(deltas))
	for _, d := range deltas {
		parts = append(parts, fmt.Sprintf("+%d %s", d.n, d.frame))
	}
	return "extra: " + strings.Join(parts, ", ")
}
