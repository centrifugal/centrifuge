//go:build integration

package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/controlpb"
	"github.com/stretchr/testify/require"
)

// ResyncOnlyLostSlots counts and observes writes only while every node strips
// observations from epochs: an older node in the cluster (a rolling upgrade)
// turns it off, so its clients never get an epoch with an observation in it.
func TestRedisLineage_CountingNeedsEveryNode(t *testing.T) {
	node := testNode(t)
	s, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	b, err := NewRedisBroker(node, RedisBrokerConfig{Prefix: getUniquePrefix(), Shards: []*RedisShard{s}, ResyncOnlyLostSlots: true})
	require.NoError(t, err)
	node.SetBroker(b)
	require.NoError(t, node.Run())
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		stopRedisBroker(b)
	})
	l := s.lineage
	require.Eventually(t, l.counting, 10*time.Second, 10*time.Millisecond)

	observedCount := func() uint64 {
		l.obsMu.Lock()
		defer l.obsMu.Unlock()
		var max uint64
		for _, o := range l.obs {
			if o.count > max {
				max = o.count
			}
		}
		return max
	}
	ch := "caps_" + randString(6)
	lineagePublish(t, b, ch, 1)
	require.NotZero(t, observedCount(), "counting: the write is observed")

	node.nodes.add(&controlpb.Node{Uid: "old-version"})
	require.False(t, l.counting())
	l.obsMu.Lock()
	clear(l.obs)
	l.obsMu.Unlock()
	sp := lineagePublish(t, b, ch, 1)
	require.NotContains(t, sp.Epoch, ";")
	require.Zero(t, observedCount(), "not counting: nothing observed")
	require.Zero(t, l.countingFor())

	node.nodes.remove("old-version")
	require.True(t, l.counting())
	lineagePublish(t, b, ch, 1)
	require.NotZero(t, observedCount())
}
