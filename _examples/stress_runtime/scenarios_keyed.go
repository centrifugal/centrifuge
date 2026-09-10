package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge"
)

// Map subscriptions and shared poll are the two keyed subscription kinds. Both
// hand a connection a set of keys rather than a stream of publications, and both
// have a handshake the plain channel scenarios never exercise: a map
// subscription pages through channel state before it goes live, a shared poll
// subscription tracks keys that a server-side loop refreshes. The scenarios here
// assert those handshakes deliver every key exactly once, that updates and
// removals reach the connection afterwards, and that neither leaks when
// connections churn through them.

// mapStateLive pages a map subscription through its state phase and then checks
// that live updates and removals keep arriving on the same subscription.
func mapStateLive(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chMap, "state")
	const keys = 25
	for i := 0; i < keys; i++ {
		if _, err := e.node.MapPublish(ctx, ch, fmt.Sprintf("k%02d", i), centrifuge.MapPublishOptions{
			Data: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}); err != nil {
			return fail("seed key %d: %v", i, err)
		}
	}

	r, err := dialRaw(e.wsURL)
	if err != nil {
		return fail("dial: %v", err)
	}
	defer r.close()
	if _, err := r.connect(newUser("map")); err != nil {
		return fail("connect: %v", err)
	}

	// Page through state. The server switches the reported phase to LIVE on the
	// last page, which is the signal the handshake is complete.
	seen := map[string]int{}
	cursor := ""
	pages := 0
	var id uint32 = 10
	for {
		id++
		pages++
		if pages > keys+5 {
			return fail("state paging did not finish after %d pages", pages)
		}
		res, _, err := r.mapSubscribe(id, ch, mapPhaseState, cursor, 10)
		if err != nil {
			return fail("page %d: %v", pages, err)
		}
		for _, p := range res.State {
			seen[p.Key]++
		}
		if res.Phase == mapPhaseLive {
			break
		}
		if res.Cursor == "" {
			return fail("page %d reported phase %d with no cursor to continue", pages, res.Phase)
		}
		cursor = res.Cursor
	}
	if len(seen) != keys {
		return fail("state delivered %d distinct keys, want %d", len(seen), keys)
	}
	for k, n := range seen {
		if n != 1 {
			return fail("key %s delivered %d times during state paging", k, n)
		}
	}

	// Live phase: an update and a removal must both arrive on the subscription.
	if _, err := e.node.MapPublish(ctx, ch, "k00", centrifuge.MapPublishOptions{Data: []byte(`{"i":999}`)}); err != nil {
		return fail("live publish: %v", err)
	}
	if _, err := e.node.MapRemove(ctx, ch, "k01", centrifuge.MapRemoveOptions{}); err != nil {
		return fail("live remove: %v", err)
	}
	var gotUpdate, gotRemoval bool
	pushes, err := r.drainUntil(10*time.Second, func(p *pushBody) bool {
		if p.Pub == nil || p.Channel != ch {
			return false
		}
		if p.Pub.Key == "k00" && !p.Pub.Removed {
			gotUpdate = true
		}
		if p.Pub.Key == "k01" && p.Pub.Removed {
			gotRemoval = true
		}
		return gotUpdate && gotRemoval
	})
	if err != nil {
		return fail("drain live pushes: %v", err)
	}
	if !gotUpdate {
		return fail("live update for k00 never arrived (%d pushes seen)", len(pushes))
	}
	if !gotRemoval {
		return fail("live removal for k01 never arrived (%d pushes seen)", len(pushes))
	}
	return okf("%d keys paged over %d state pages, then live update and removal delivered", keys, pages)
}

// mapTTLRemoval checks that keys expiring by TTL become removals on a live
// subscription - the cleanup path, which nothing else in the suite drives.
func mapTTLRemoval(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chMapTTL, "ttl")
	const keys = 6
	for i := 0; i < keys; i++ {
		if _, err := e.node.MapPublish(ctx, ch, fmt.Sprintf("t%d", i), centrifuge.MapPublishOptions{
			Data: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}); err != nil {
			return fail("seed key %d: %v", i, err)
		}
	}
	r, err := dialRaw(e.wsURL)
	if err != nil {
		return fail("dial: %v", err)
	}
	defer r.close()
	if _, err := r.connect(newUser("mapttl")); err != nil {
		return fail("connect: %v", err)
	}
	res, _, err := r.mapSubscribe(10, ch, mapPhaseState, "", 100)
	if err != nil {
		return fail("subscribe: %v", err)
	}
	if len(res.State) != keys {
		return fail("state carried %d keys, want %d", len(res.State), keys)
	}

	// KeyTTL on "mapttl:" channels is 3s; give the cleanup worker room past it.
	removed := map[string]bool{}
	if _, err := r.drainUntil(25*time.Second, func(p *pushBody) bool {
		if p.Pub != nil && p.Channel == ch && p.Pub.Removed {
			removed[p.Pub.Key] = true
		}
		return len(removed) == keys
	}); err != nil {
		return fail("drain: %v", err)
	}
	if len(removed) != keys {
		return fail("%d/%d keys reported as removed after TTL", len(removed), keys)
	}
	return okf("all %d keys expired by TTL and were delivered as removals", keys)
}

// mapChurn runs connections through the whole map handshake while keys change
// underneath them, then checks the hub let go of everything.
func mapChurn(ctx context.Context, e *env) (string, error) {
	channels := []string{newChannel(chMap, "churn-a"), newChannel(chMap, "churn-b")}
	for _, ch := range channels {
		for i := 0; i < 20; i++ {
			if _, err := e.node.MapPublish(ctx, ch, fmt.Sprintf("k%02d", i), centrifuge.MapPublishOptions{
				Data: []byte(`{"seed":true}`),
			}); err != nil {
				return fail("seed: %v", err)
			}
		}
	}

	deadline := time.Now().Add(loadDur)
	var wg sync.WaitGroup
	var cycles, failures atomic.Int64
	var firstErr atomic.Value

	// Key churn.
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := 0
			for time.Now().Before(deadline) {
				ch := channels[n%len(channels)]
				key := fmt.Sprintf("k%02d", n%25)
				var err error
				if n%5 == 0 {
					_, err = e.node.MapRemove(ctx, ch, key, centrifuge.MapRemoveOptions{})
				} else {
					_, err = e.node.MapPublish(ctx, ch, key, centrifuge.MapPublishOptions{
						Data: []byte(fmt.Sprintf(`{"n":%d}`, n)),
					})
				}
				if err != nil {
					failures.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("key churn: %v", err))
					return
				}
				n++
			}
		}(i)
	}

	// Subscribe / page / drop churn.
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := 0
			for time.Now().Before(deadline) {
				ch := channels[(id+n)%len(channels)]
				r, err := dialRaw(e.wsURL)
				if err != nil {
					failures.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("dial: %v", err))
					return
				}
				if _, err := r.connect(newUser("mapchurn")); err != nil {
					r.close()
					failures.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("connect: %v", err))
					return
				}
				cursor := ""
				var id32 uint32 = 10
				for page := 0; page < 6; page++ {
					id32++
					res, _, err := r.mapSubscribe(id32, ch, mapPhaseState, cursor, 5)
					if err != nil {
						r.close()
						failures.Add(1)
						firstErr.CompareAndSwap(nil, fmt.Sprintf("map subscribe: %v", err))
						return
					}
					if res.Phase == mapPhaseLive {
						break
					}
					cursor = res.Cursor
					if cursor == "" {
						break
					}
				}
				r.close()
				cycles.Add(1)
				n++
			}
		}(i)
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return fail("%d failures, first: %s", failures.Load(), v.(string))
	}
	for _, ch := range channels {
		if !waitFor(10*time.Second, func() bool { return e.node.Hub().NumSubscribers(ch) == 0 }) {
			return fail("channel %s still has %d subscribers after churn", ch, e.node.Hub().NumSubscribers(ch))
		}
	}
	return okf("%d map subscribe cycles over %s, hub drained", cycles.Load(), loadDur)
}

// sharedPollTrack drives the shared poll refresh loop: tracked keys must be
// refreshed, an untracked key must go quiet, and removals must be delivered.
func sharedPollTrack(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chPoll, "track")
	keys := []string{"a", "b", "c"}
	for i, k := range keys {
		pollStore.set(ch, k, uint64(i+1))
	}

	r, err := dialRaw(e.wsURL)
	if err != nil {
		return fail("dial: %v", err)
	}
	defer r.close()
	if _, err := r.connect(newUser("poll")); err != nil {
		return fail("connect: %v", err)
	}
	if _, err := r.pollSubscribe(10, ch); err != nil {
		return fail("subscribe: %v", err)
	}
	if err := r.track(11, ch, keys); err != nil {
		return fail("track: %v", err)
	}

	// Every tracked key must be delivered at its current version.
	got := map[string]uint64{}
	if _, err := r.drainUntil(10*time.Second, func(p *pushBody) bool {
		if p.Pub != nil && p.Channel == ch && !p.Pub.Removed {
			got[p.Pub.Key] = p.Pub.Version
		}
		return len(got) == len(keys)
	}); err != nil {
		return fail("drain: %v", err)
	}
	if len(got) != len(keys) {
		return fail("refresh delivered %d/%d tracked keys", len(got), len(keys))
	}

	// Untrack "a", then move every key: only the still-tracked ones may arrive.
	if err := r.untrack(12, ch, []string{"a"}); err != nil {
		return fail("untrack: %v", err)
	}
	for _, k := range keys {
		pollStore.set(ch, k, 100)
	}
	pollStore.remove(ch, "c")

	updated := map[string]bool{}
	removals := map[string]bool{}
	if _, err := r.drainUntil(10*time.Second, func(p *pushBody) bool {
		if p.Pub == nil || p.Channel != ch {
			return false
		}
		if p.Pub.Removed {
			removals[p.Pub.Key] = true
		} else if p.Pub.Version >= 100 {
			updated[p.Pub.Key] = true
		}
		return updated["b"] && removals["c"]
	}); err != nil {
		return fail("drain after untrack: %v", err)
	}
	if !updated["b"] {
		return fail("tracked key b never refreshed after its version moved")
	}
	if !removals["c"] {
		return fail("removal of tracked key c never delivered")
	}
	if updated["a"] {
		return fail("untracked key a kept being refreshed")
	}
	return okf("%d keys refreshed, untracked key went quiet, removal delivered", len(keys))
}

// sharedPollChurn churns connections through subscribe/track/untrack/drop while
// the refresh loop runs, then checks the manager let go of every channel.
func sharedPollChurn(ctx context.Context, e *env) (string, error) {
	channels := []string{newChannel(chPoll, "churn-a"), newChannel(chPoll, "churn-b")}
	for _, ch := range channels {
		for i := 0; i < 30; i++ {
			pollStore.set(ch, fmt.Sprintf("k%02d", i), uint64(i))
		}
	}

	deadline := time.Now().Add(loadDur)
	var wg sync.WaitGroup
	var cycles, failures atomic.Int64
	var firstErr atomic.Value

	// Key churn under the refresh loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := uint64(0)
		for time.Now().Before(deadline) {
			for _, ch := range channels {
				pollStore.set(ch, fmt.Sprintf("k%02d", n%30), 1000+n)
			}
			n++
			time.Sleep(5 * time.Millisecond)
		}
	}()

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := 0
			for time.Now().Before(deadline) {
				ch := channels[(id+n)%len(channels)]
				r, err := dialRaw(e.wsURL)
				if err != nil {
					failures.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("dial: %v", err))
					return
				}
				if _, err := r.connect(newUser("pollchurn")); err != nil {
					r.close()
					failures.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("connect: %v", err))
					return
				}
				if _, err := r.pollSubscribe(10, ch); err != nil {
					r.close()
					failures.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("subscribe: %v", err))
					return
				}
				keys := []string{
					fmt.Sprintf("k%02d", n%30),
					fmt.Sprintf("k%02d", (n+7)%30),
					fmt.Sprintf("k%02d", (n+13)%30),
				}
				if err := r.track(11, ch, keys); err != nil {
					r.close()
					failures.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("track: %v", err))
					return
				}
				switch n % 3 {
				case 0:
					// Untrack, then drop.
					if err := r.untrack(12, ch, keys); err != nil {
						r.close()
						failures.Add(1)
						firstErr.CompareAndSwap(nil, fmt.Sprintf("untrack: %v", err))
						return
					}
				case 1:
					// Drop while a refresh cycle is in flight.
					_, _ = r.drainUntil(30*time.Millisecond, nil)
				case 2:
					// Re-track the same keys before dropping.
					if err := r.track(13, ch, keys); err != nil {
						r.close()
						failures.Add(1)
						firstErr.CompareAndSwap(nil, fmt.Sprintf("re-track: %v", err))
						return
					}
				}
				r.close()
				cycles.Add(1)
				n++
			}
		}(i)
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return fail("%d failures, first: %s", failures.Load(), v.(string))
	}
	for _, ch := range channels {
		if !waitFor(15*time.Second, func() bool { return e.node.Hub().NumSubscribers(ch) == 0 }) {
			return fail("channel %s still has %d subscribers after churn", ch, e.node.Hub().NumSubscribers(ch))
		}
	}
	return okf("%d shared poll cycles over %s, hub drained", cycles.Load(), loadDur)
}

// redisMapCrossNode checks a map subscription is a cluster feature: a key
// written through one node reaches a connection subscribed on the other, and the
// state a fresh subscriber reads there is the same.
func redisMapCrossNode(ctx context.Context, e *env) (string, error) {
	if !e.redisEnabled() {
		return okf("skipped (no redis)")
	}
	ch := newChannel(chMap, "xnode")
	const keys = 8
	for i := 0; i < keys; i++ {
		if _, err := e.redisA.MapPublish(ctx, ch, fmt.Sprintf("k%d", i), centrifuge.MapPublishOptions{
			Data: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}); err != nil {
			return fail("publish on node A: %v", err)
		}
	}

	// Subscribe on node B and read the state written through node A.
	r, err := dialRaw(e.redisBWS)
	if err != nil {
		return fail("dial node B: %v", err)
	}
	defer r.close()
	if _, err := r.connect(newUser("mapx")); err != nil {
		return fail("connect node B: %v", err)
	}
	res, _, err := r.mapSubscribe(10, ch, mapPhaseState, "", 100)
	if err != nil {
		return fail("subscribe node B: %v", err)
	}
	if len(res.State) != keys {
		return fail("node B state has %d keys, node A wrote %d", len(res.State), keys)
	}

	// A live update through node A must reach the node B connection over Redis.
	if _, err := e.redisA.MapPublish(ctx, ch, "k0", centrifuge.MapPublishOptions{Data: []byte(`{"i":777}`)}); err != nil {
		return fail("cross-node publish: %v", err)
	}
	if _, err := e.redisA.MapRemove(ctx, ch, "k1", centrifuge.MapRemoveOptions{}); err != nil {
		return fail("cross-node remove: %v", err)
	}
	var gotUpdate, gotRemoval bool
	if _, err := r.drainUntil(15*time.Second, func(p *pushBody) bool {
		if p.Pub == nil || p.Channel != ch {
			return false
		}
		if p.Pub.Key == "k0" && !p.Pub.Removed {
			gotUpdate = true
		}
		if p.Pub.Key == "k1" && p.Pub.Removed {
			gotRemoval = true
		}
		return gotUpdate && gotRemoval
	}); err != nil {
		return fail("drain: %v", err)
	}
	if !gotUpdate {
		return fail("update written on node A never reached node B")
	}
	if !gotRemoval {
		return fail("removal written on node A never reached node B")
	}
	return okf("%d keys read on node B, cross-node update and removal delivered", keys)
}
