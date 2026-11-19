//go:build integration

package centrifuge

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

var lineageSingle = lineageRedisTest{"rd_single", false, 6379, true}

func lineageKeyValue(t *testing.T, b *RedisBroker, ch string) (string, []string) {
	t.Helper()
	s := b.shards[0].shard
	key := b.lin(s).tokenKey(string(b.historyMetaKey(s, ch)))
	vals, err := s.client.Do(context.Background(), s.client.B().Hmget().Key(key).Field("t", "p").Build()).ToArray()
	require.NoError(t, err)
	v, _ := vals[0].ToString()
	p, _ := vals[1].ToString()
	return v, strings.Fields(p)
}

func setLineageKey(t *testing.T, b *RedisBroker, ch, value, preds string) {
	t.Helper()
	s := b.shards[0].shard
	key := b.lin(s).tokenKey(string(b.historyMetaKey(s, ch)))
	require.NoError(t, s.client.Do(context.Background(), s.client.B().Hset().Key(key).FieldValue().
		FieldValue("t", value).FieldValue("p", preds).Build()).Error())
}

func lineageStaleRotations(b *RedisBroker) int {
	st := &b.lin(b.shards[0].shard).stats
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.stale
}

// A rotation records the replaced token in the lineage key.
func TestRedisLineage_RotationKeepsReplacedTokens(t *testing.T) {
	b := newLineageTestBroker(t, lineageSingle, false)
	ch := "lineage_preds_" + randString(6)
	lineagePublish(t, b, ch, 1)
	first, _ := lineageKeyValue(t, b, ch)
	flushScripts(t, b.shards[0].shard)
	lineagePublish(t, b, ch, 1)
	second, preds := lineageKeyValue(t, b, ch)
	require.NotEqual(t, first, second)
	require.Equal(t, first, preds[0], "the replaced token, newest first")
	require.True(t, b.lin(b.shards[0].shard).leftTokens().has(first))
}

// A server presenting a token this node saw replaced is a copy that missed a
// rotation (an old primary that took a full copy before the rotation reached
// it, promoted back with our scripts): its lineage rotates instead of being
// followed. Emulated: the lineage key goes back to the replaced token.
func TestRedisLineage_StaleCopyRotates(t *testing.T) {
	b := newLineageTestBroker(t, lineageSingle, false)
	ch := "lineage_stale_" + randString(6)
	lineagePublish(t, b, ch, 2)
	old, _ := lineageKeyValue(t, b, ch)
	flushScripts(t, b.shards[0].shard) // a new lineage: the token rotates
	before := lineagePublish(t, b, ch, 1)

	// The stale copy: data (meta, lineage key) under the replaced token.
	setLineageKey(t, b, ch, old, "")
	s := b.shards[0].shard
	require.NoError(t, s.client.Do(context.Background(), s.client.B().Hset().Key(string(b.historyMetaKey(s, ch))).
		FieldValue().FieldValue("i", old).Build()).Error())

	after := lineagePublish(t, b, ch, 1)
	require.NotEqual(t, before.Epoch, after.Epoch, "the stale copy's streams start a new epoch")
	require.Equal(t, uint64(1), after.Offset)
	require.Equal(t, 1, lineageStaleRotations(b))
	now, _ := lineageKeyValue(t, b, ch)
	require.NotEqual(t, old, now)
}

// A node that never used a token learns it was replaced from the lineage key
// of the token that replaced it.
func TestRedisLineage_LearnsReplacedTokensFromKey(t *testing.T) {
	b1 := newLineageTestBroker(t, lineageSingle, false)
	ch := "lineage_learn_" + randString(6)
	lineagePublish(t, b1, ch, 1)
	old, _ := lineageKeyValue(t, b1, ch)
	flushScripts(t, b1.shards[0].shard)
	lineagePublish(t, b1, ch, 1) // b1 rotates: old is replaced

	// A new node, same prefix: it only ever saw the new token.
	b2 := newLineageTestBrokerPrefix(t, lineageSingle, false, b1.config.Prefix)
	before := lineagePublish(t, b2, ch, 1)
	require.True(t, b2.lin(b2.shards[0].shard).leftTokens().has(old))

	setLineageKey(t, b2, ch, old, "")
	after := lineagePublish(t, b2, ch, 1)
	require.NotEqual(t, before.Epoch, after.Epoch)
	require.Equal(t, 1, lineageStaleRotations(b2))
}

// Nodes pass the replaced tokens they know with their node info: a node that
// starts later learns them before it could meet a stale copy.
func TestRedisLineage_ReplacedTokensExchanged(t *testing.T) {
	prefix := getUniquePrefix()
	n1 := testNode(t)
	b1 := newDetectingTestRedisBroker(t, n1, prefix)
	t.Cleanup(func() { _ = n1.Shutdown(context.Background()); stopRedisBroker(b1) })
	token := "gexchanged" + randString(6)
	n1.lineageLeft.add(token)

	n2 := testNode(t)
	b2 := newDetectingTestRedisBroker(t, n2, prefix)
	t.Cleanup(func() { _ = n2.Shutdown(context.Background()); stopRedisBroker(b2) })
	require.Eventually(t, func() bool { return n2.lineageLeft.has(token) }, 10*time.Second, 20*time.Millisecond)
}

// The idle probe opens the gate of a server that changed while the node made
// no calls: a restart (emulated: the script cache flushed) rotates before any
// traffic.
func TestRedisLineage_IdleProbeOpensGate(t *testing.T) {
	setLineageProbeInterval(t, 50*time.Millisecond)
	b := newLineageTestBroker(t, lineageSingle, false)
	ch := "lineage_probe_" + randString(6)
	lineagePublish(t, b, ch, 1)
	rot := lineageRotations(b)
	flushScripts(t, b.shards[0].shard)
	require.Eventually(t, func() bool { return lineageRotations(b) > rot }, 5*time.Second, 10*time.Millisecond)
}

// Each mode stamps its own field: switching a broker between the gate and
// INFO (or nodes in both modes during a rollout) keeps every epoch.
func TestRedisLineage_ModeSwitchKeepsEpochs(t *testing.T) {
	gate := newLineageTestBroker(t, lineageSingle, false)
	mode, err := gate.lin(gate.shards[0].shard).mode(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, lineageGate, mode)
	ch := "lineage_modes_" + randString(6)
	first := lineagePublish(t, gate, ch, 2)

	info := newLineageTestBrokerPrefix(t, lineageSingle, true, gate.config.Prefix)
	if m, _ := info.lin(info.shards[0].shard).mode(context.Background(), true); m != lineageInfo {
		t.Skip("scripts can't call INFO")
	}
	viaInfo := lineagePublish(t, info, ch, 1)
	require.Equal(t, first.Epoch, viaInfo.Epoch, "the INFO mode adopts the meta")
	require.Equal(t, uint64(3), viaInfo.Offset)

	viaGate := lineagePublish(t, gate, ch, 1)
	require.Equal(t, first.Epoch, viaGate.Epoch, "and the gate keeps it")
	require.Equal(t, uint64(4), viaGate.Offset)
}

// A reused offset in one channel resets every stream on the server: a client
// holding a position in another channel - offline, with no subscriber to see
// that channel's loss - can't recover across it either.
func TestRedisBroker_ReusedOffsetResetsServer(t *testing.T) {
	node, b, insufficient := newWatermarkTestNode(t)
	watched := "watermark_watched_" + randString(8)
	offline := "watermark_offline_" + randString(8)

	client := newTestConnectedClientV2(t, node, "42")
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: watched, Recover: true}, &protocol.Command{}, time.Now(), rw.rw))
	for i := 0; i < 3; i++ {
		_, err := node.Publish(watched, []byte(`{}`), WithHistory(10, time.Minute))
		require.NoError(t, err)
	}
	var offlinePos StreamPosition
	for i := 0; i < 3; i++ {
		res, err := node.Publish(offline, []byte(`{}`), WithHistory(10, time.Minute))
		require.NoError(t, err)
		offlinePos = res.StreamPosition
	}
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.channels[watched].streamPosition.Offset == 3
	}, 5*time.Second, 10*time.Millisecond)

	// Lossy failover: both channels lost their last two publications.
	rewindStream(t, b, watched, 1)
	rewindStream(t, b, offline, 1)
	_, err := node.Publish(watched, []byte(`{}`), WithHistory(10, time.Minute))
	require.NoError(t, err)
	select {
	case <-insufficient:
	case <-time.After(5 * time.Second):
		t.Fatal("the watched channel's subscriber must resync")
	}
	// The offline client's channel: new publications reach offset 3 again, but
	// its old position can't recover - the server's lineage rotated.
	require.Eventually(t, func() bool {
		_, err := node.recoverHistory(offline, offlinePos, time.Hour, 0)
		return err != nil
	}, 5*time.Second, 10*time.Millisecond)
	for i := 0; i < 3; i++ {
		_, err := node.Publish(offline, []byte(`{}`), WithHistory(10, time.Minute))
		require.NoError(t, err)
	}
	_, err = node.recoverHistory(offline, offlinePos, time.Hour, 0)
	require.ErrorIs(t, err, ErrorUnrecoverablePosition)
}

// Deployments sharing a Redis server (different prefixes) have their own
// lineages: one's break (here a stale copy only it detects) resets only its
// own streams.
func TestRedisLineage_PrefixesIndependent(t *testing.T) {
	a := newLineageTestBroker(t, lineageSingle, false)
	b := newLineageTestBroker(t, lineageSingle, false)
	ch := "lineage_prefixes_" + randString(6)
	aBefore := lineagePublish(t, a, ch, 2)
	bBefore := lineagePublish(t, b, ch, 2)
	ka, _ := lineageKeyValue(t, a, ch)
	kb, _ := lineageKeyValue(t, b, ch)
	require.NotEqual(t, ka, kb, "a token per prefix")

	// A's lineage breaks: its lineage key is gone (a SCRIPT FLUSH would hit
	// every user of the server).
	sA := a.shards[0].shard
	require.NoError(t, sA.client.Do(context.Background(), sA.client.B().Del().Key(a.lin(sA).tokenKey(string(a.historyMetaKey(sA, ch)))).Build()).Error())
	aAfter := lineagePublish(t, a, ch, 1)
	require.NotEqual(t, aBefore.Epoch, aAfter.Epoch)

	bAfter := lineagePublish(t, b, ch, 1)
	require.Equal(t, bBefore.Epoch, bAfter.Epoch, "the other deployment keeps its streams")
	require.Equal(t, uint64(3), bAfter.Offset)
}

// With LoadSHA1 (FIPS-only mode) the gate works the same: digests come from
// the server without loading the gated scripts (SCRIPT LOAD would open the
// gate on a server it hasn't checked), and a server that lost its scripts
// still starts new epochs.
func TestRedisLineage_LoadSHA1(t *testing.T) {
	node := testNode(t)
	s, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	b, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:           getUniquePrefix(),
		Shards:           []*RedisShard{s},
		LoadSHA1:         true,
		DetectLostWrites: true,
	})
	require.NoError(t, err)
	node.SetBroker(b)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()); stopRedisBroker(b) })

	ch := "lineage_fips_" + randString(6)
	before := lineagePublish(t, b, ch, 2)
	require.Equal(t, uint64(2), before.Offset)

	l := b.lin(s)
	token := l.token(l.slotIndex(string(b.historyMetaKey(s, ch))))
	require.NotEmpty(t, token)
	cached, ok := b.addHistoryStreamScript.shas.Load(token)
	require.True(t, ok)
	server, err := s.client.Do(context.Background(), s.client.B().Eval().Script(lineageSha1Source).
		Numkeys(0).Arg(b.addHistoryStreamScript.gatedSource(token)).Build()).ToString()
	require.NoError(t, err)
	require.Equal(t, server, cached, "the digest is the server's")

	flushScripts(t, s)
	after := lineagePublish(t, b, ch, 1)
	require.NotEqual(t, before.Epoch, after.Epoch, "a server without the scripts starts a new epoch")
	require.Equal(t, uint64(1), after.Offset)
}

// setLineageProbeInterval sets the idle probe's interval for a test and
// restores it when the test ends.
func setLineageProbeInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := lineageProbeInterval()
	lineageProbeIntervalNs.Store(int64(d))
	t.Cleanup(func() { lineageProbeIntervalNs.Store(int64(prev)) })
}

// newDetectingTestRedisBroker is NewTestRedisBroker (streams, port 6379) with
// DetectLostWrites.
func newDetectingTestRedisBroker(tb testing.TB, n *Node, prefix string) *RedisBroker {
	tb.Helper()
	s, err := NewRedisShard(n, testSingleRedisConf(6379))
	require.NoError(tb, err)
	e, err := NewRedisBroker(n, RedisBrokerConfig{
		Prefix:           prefix,
		Shards:           []*RedisShard{s},
		DetectLostWrites: true,
	})
	require.NoError(tb, err)
	n.SetBroker(e)
	require.NoError(tb, n.Run())
	return e
}
