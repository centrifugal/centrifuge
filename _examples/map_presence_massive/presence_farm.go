package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
)

// presenceFarmConfig describes a synthetic map_clients presence load.
// Entries are pushed directly via the map broker — no real WebSocket
// clients — so we can stress the protocol with hundreds of thousands of
// entries on a single process.
//
// After the initial bulk fill, churn runs as paired "replace" events: each
// tick removes one random live entry and publishes one random non-live
// entry from the fixed pool. Population stays exactly at InitialCount, and
// every key is reused over time so each grid cell flickers with activity.
type presenceFarmConfig struct {
	Channel      string
	PoolSize     int // total stable id pool — keys are c_0..c_<PoolSize-1>
	InitialCount int
	ChurnPerSec  int
}

func runPresenceFarm(ctx context.Context, node *centrifuge.Node, cfg presenceFarmConfig) {
	if cfg.Channel == "" {
		cfg.Channel = "clients:massive"
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 102400
	}
	if cfg.InitialCount <= 0 {
		cfg.InitialCount = cfg.PoolSize - cfg.PoolSize/40 // ~97.5% full
	}
	if cfg.InitialCount > cfg.PoolSize {
		log.Printf("massive farm: count %d exceeds pool %d, clamping", cfg.InitialCount, cfg.PoolSize)
		cfg.InitialCount = cfg.PoolSize
	}
	if cfg.ChurnPerSec <= 0 {
		cfg.ChurnPerSec = 200
	}

	keyOf := func(idx int) string { return fmt.Sprintf("c_%d", idx) }

	publish := func(idx int) {
		key := keyOf(idx)
		_, err := node.MapPublish(ctx, cfg.Channel, key, centrifuge.MapPublishOptions{
			ClientInfo: &centrifuge.ClientInfo{
				ClientID: key,
				UserID:   "u_" + key,
			},
		})
		if err != nil && ctx.Err() == nil {
			log.Printf("massive farm publish err: %v", err)
		}
	}

	remove := func(idx int) {
		_, err := node.MapRemove(ctx, cfg.Channel, keyOf(idx), centrifuge.MapRemoveOptions{})
		if err != nil && ctx.Err() == nil {
			log.Printf("massive farm remove err: %v", err)
		}
	}

	// Shuffle 0..PoolSize-1 once. The first InitialCount become live, the
	// rest become the not-live pool.
	all := make([]int, cfg.PoolSize)
	for i := range all {
		all[i] = i
	}
	rand.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })

	var (
		mu      sync.Mutex
		live    = all[:cfg.InitialCount]
		notLive = all[cfg.InitialCount:]
	)

	// popRandom removes a random element from a slice and returns it. Caller
	// must hold mu.
	popRandom := func(s *[]int) (int, bool) {
		if len(*s) == 0 {
			return 0, false
		}
		i := rand.IntN(len(*s))
		v := (*s)[i]
		(*s)[i] = (*s)[len(*s)-1]
		*s = (*s)[:len(*s)-1]
		return v, true
	}

	log.Printf("massive farm: populating %d entries on %q (pool=%d) ...", cfg.InitialCount, cfg.Channel, cfg.PoolSize)
	start := time.Now()
	for _, idx := range live {
		if ctx.Err() != nil {
			return
		}
		publish(idx)
	}
	log.Printf("massive farm: populated %d entries in %s", cfg.InitialCount, time.Since(start))

	// If everyone is live there's nothing to swap with — disable churn.
	if len(notLive) == 0 {
		<-ctx.Done()
		return
	}

	churnTick := time.NewTicker(time.Second / time.Duration(cfg.ChurnPerSec))
	defer churnTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-churnTick.C:
			mu.Lock()
			leaveIdx, lok := popRandom(&live)
			joinIdx, jok := popRandom(&notLive)
			if lok {
				notLive = append(notLive, leaveIdx)
			}
			if jok {
				live = append(live, joinIdx)
			}
			mu.Unlock()
			if lok {
				remove(leaveIdx)
			}
			if jok {
				publish(joinIdx)
			}
		}
	}
}
