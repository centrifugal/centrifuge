//go:build integration

package centrifuge

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

type lineageRedisTest struct {
	Name       string
	UseCluster bool
	Port       int
	// Gate: the server can use the script-cache gate (Redis 7+, Valkey).
	Gate bool
}

var lineageRedisTests = []lineageRedisTest{
	{"rd_single", false, 6379, true},
	{"vk_single", false, 16379, true},
	{"df_single", false, 36379, false},
	{"rd_cluster", true, 7001, true},
	{"vk_cluster", true, 17000, true},
}

func newLineageTestBroker(t *testing.T, tt lineageRedisTest, allowInfo bool) *RedisBroker {
	t.Helper()
	return newLineageTestBrokerPrefix(t, tt, allowInfo, getUniquePrefix())
}

func newLineageTestBrokerPrefix(t *testing.T, tt lineageRedisTest, allowInfo bool, prefix string) *RedisBroker {
	t.Helper()
	node := testNode(t)
	var conf RedisShardConfig
	if tt.UseCluster {
		var addrs []string
		for i := 0; i < 3; i++ {
			addrs = append(addrs, net.JoinHostPort("127.0.0.1", strconv.Itoa(tt.Port+i)))
		}
		conf = RedisShardConfig{ClusterAddresses: addrs, IOTimeout: 10 * time.Second}
	} else {
		conf = testSingleRedisConf(tt.Port)
	}
	s, err := NewRedisShard(node, conf)
	require.NoError(t, err)
	b, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:                   prefix,
		Shards:                   []*RedisShard{s},
		UseInfoCommandForLineage: allowInfo,
	})
	require.NoError(t, err)
	node.SetBroker(b)
	require.NoError(t, node.Run())
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		stopRedisBroker(b)
	})
	return b
}

// primaries returns a client per primary of the shard (the shard client
// outside Redis Cluster).
func lineagePrimaries(t *testing.T, s *RedisShard) []rueidis.Client {
	t.Helper()
	if !s.isCluster {
		return []rueidis.Client{s.client}
	}
	var out []rueidis.Client
	for _, c := range s.client.Nodes() {
		role, err := c.Do(context.Background(), c.B().Role().Build()).ToArray()
		require.NoError(t, err)
		r, _ := role[0].ToString()
		if r == "master" {
			out = append(out, c)
		}
	}
	require.NotEmpty(t, out)
	return out
}

// flushScripts empties the script cache of every primary: what a restart or a
// promotion leaves behind.
func flushScripts(t *testing.T, s *RedisShard) {
	t.Helper()
	for _, c := range lineagePrimaries(t, s) {
		require.NoError(t, c.Do(context.Background(), c.B().ScriptFlush().Build()).Error())
	}
}

func lineagePublish(t *testing.T, b *RedisBroker, ch string, n int) StreamPosition {
	t.Helper()
	var sp StreamPosition
	for i := 0; i < n; i++ {
		res, err := b.Publish(ch, []byte(`{"n":`+strconv.Itoa(i)+`}`), PublishOptions{
			HistorySize: 10, HistoryTTL: time.Minute,
		})
		require.NoError(t, err)
		sp = res.StreamPosition
	}
	return sp
}

func TestRedisLineage_Mode(t *testing.T) {
	for _, tt := range lineageRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			b := newLineageTestBroker(t, tt, false)
			s := b.shards[0].shard
			mode, err := b.lin(s).mode(context.Background(), b.config.UseInfoCommandForLineage)
			require.NoError(t, err)
			if tt.Gate {
				require.Equal(t, lineageGate, mode)
			} else {
				require.Equal(t, lineageNone, mode, "no INFO without UseInfoCommandForLineage")
			}
		})
	}
}

func TestRedisLineage_DragonflyWithInfo(t *testing.T) {
	tt := lineageRedisTest{"df_single", false, 36379, false}
	b := newLineageTestBroker(t, tt, true)
	s := b.shards[0].shard
	mode, err := b.lin(s).mode(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, lineageInfo, mode)

	ch := "lineage_df_" + randString(6)
	sp := lineagePublish(t, b, ch, 2)
	require.Equal(t, uint64(2), sp.Offset)
	stamp, err := s.client.Do(context.Background(), s.client.B().Hget().Key(string(b.historyMetaKey(s, ch))).Field(lineageStampInfo).Build()).ToString()
	require.NoError(t, err)
	require.Equal(t, byte('r'), stamp[0], "stamped with master_replid")
}

// A new lineage (a script cache emptied by a restart or a promotion) starts a
// new epoch for every stream on the server.
func TestRedisLineage_NewLineageStartsNewEpoch(t *testing.T) {
	for _, tt := range lineageRedisTests {
		if !tt.Gate {
			continue
		}
		t.Run(tt.Name, func(t *testing.T) {
			b := newLineageTestBroker(t, tt, false)
			s := b.shards[0].shard
			ch := "lineage_new_" + randString(6)

			before := lineagePublish(t, b, ch, 3)
			require.Equal(t, uint64(3), before.Offset)

			// The same lineage keeps the epoch.
			same := lineagePublish(t, b, ch, 1)
			require.Equal(t, before.Epoch, same.Epoch)
			require.Equal(t, uint64(4), same.Offset)

			flushScripts(t, s)

			// A read after the new lineage gives a new epoch and drops the old history.
			pubs, sp, err := b.History(ch, HistoryOptions{Filter: HistoryFilter{Limit: -1}})
			require.NoError(t, err)
			require.NotEqual(t, before.Epoch, sp.Epoch)
			require.Zero(t, sp.Offset)
			require.Empty(t, pubs)

			after := lineagePublish(t, b, ch, 1)
			require.Equal(t, sp.Epoch, after.Epoch)
			require.Equal(t, uint64(1), after.Offset)
		})
	}
}

// centrifugo#1319: a lossy failover keeps the epoch and moves the top offset
// back. Emulated by rolling the offset back together with a new lineage: the
// next publication must not reuse an offset under the old epoch.
func TestRedisLineage_LossyFailoverEmulated(t *testing.T) {
	for _, tt := range lineageRedisTests {
		if !tt.Gate {
			continue
		}
		t.Run(tt.Name, func(t *testing.T) {
			b := newLineageTestBroker(t, tt, false)
			s := b.shards[0].shard
			ch := "lineage_loss_" + randString(6)
			before := lineagePublish(t, b, ch, 6)

			metaKey := string(b.historyMetaKey(s, ch))
			require.NoError(t, s.client.Do(context.Background(), s.client.B().Hset().Key(metaKey).FieldValue().FieldValue("s", "3").Build()).Error())
			flushScripts(t, s)

			after := lineagePublish(t, b, ch, 1)
			require.NotEqual(t, before.Epoch, after.Epoch, "a reused offset under the old epoch would be skipped by live clients")
			require.Equal(t, uint64(1), after.Offset)

			// A client holding the old position doesn't recover across the new epoch.
			_, sp, err := b.History(ch, HistoryOptions{Filter: HistoryFilter{Since: &before, Limit: -1}})
			require.NoError(t, err)
			require.NotEqual(t, before.Epoch, sp.Epoch)
		})
	}
}

// Metas written before lineage detection (no stamp) are adopted, not reset.
func TestRedisLineage_AdoptsUnstampedMeta(t *testing.T) {
	for _, tt := range lineageRedisTests {
		if !tt.Gate {
			continue
		}
		t.Run(tt.Name, func(t *testing.T) {
			b := newLineageTestBroker(t, tt, false)
			s := b.shards[0].shard
			ch := "lineage_adopt_" + randString(6)
			metaKey := string(b.historyMetaKey(s, ch))
			require.NoError(t, s.client.Do(context.Background(),
				s.client.B().Hset().Key(metaKey).FieldValue().FieldValue("e", "oldepoch").FieldValue("s", "5").Build()).Error())

			sp := lineagePublish(t, b, ch, 1)
			require.Equal(t, "oldepoch", sp.Epoch)
			require.Equal(t, uint64(6), sp.Offset)
			stamp, err := s.client.Do(context.Background(), s.client.B().Hget().Key(metaKey).Field("i").Build()).ToString()
			require.NoError(t, err)
			require.NotEmpty(t, stamp)
		})
	}
}

// A lost token key (eviction, a manual delete) can't let a stale meta pass.
// Outside Redis Cluster nothing else vouches for the server: a new token, a new
// epoch. In Redis Cluster other slots of the server still hold its token and
// marker, so its lineage is intact: the slot is adopted and keeps its epoch.
func TestRedisLineage_TokenKeyLost(t *testing.T) {
	for _, tt := range lineageRedisTests {
		if !tt.Gate {
			continue
		}
		t.Run(tt.Name, func(t *testing.T) {
			b := newLineageTestBroker(t, tt, false)
			s := b.shards[0].shard
			ch := "lineage_token_" + randString(6)
			before := lineagePublish(t, b, ch, 2)
			tokenKey := b.lin(s).tokenKey(string(b.historyMetaKey(s, ch)))
			require.NoError(t, s.client.Do(context.Background(), s.client.B().Del().Key(tokenKey).Build()).Error())
			after := lineagePublish(t, b, ch, 1)
			if tt.UseCluster {
				require.Equal(t, before.Epoch, after.Epoch, "the server's lineage is intact")
				require.Equal(t, uint64(3), after.Offset)
			} else {
				require.NotEqual(t, before.Epoch, after.Epoch)
				require.Equal(t, uint64(1), after.Offset)
			}
		})
	}
}

// A new broker on the same lineage (a Centrifugo restart or upgrade) keeps
// every epoch: the marker script is still loaded, so scripts missing on the
// server (a newer version's) load without a rotation.
func TestRedisLineage_SameLineageKeepsEpoch(t *testing.T) {
	for _, tt := range lineageRedisTests {
		if !tt.Gate {
			continue
		}
		t.Run(tt.Name, func(t *testing.T) {
			b1 := newLineageTestBroker(t, tt, false)
			ch := "lineage_restart_" + randString(6)
			before := lineagePublish(t, b1, ch, 2)

			s1 := b1.shards[0].shard
			tokenKey := b1.lin(s1).tokenKey(string(b1.historyMetaKey(s1, ch)))
			token, err := s1.client.Do(context.Background(), s1.client.B().Hget().Key(tokenKey).Field("t").Build()).ToString()
			require.NoError(t, err)
			for _, c := range lineagePrimaries(t, s1) {
				require.NoError(t, c.Do(context.Background(), c.B().ScriptFlush().Build()).Error())
				require.NoError(t, c.Do(context.Background(), c.B().ScriptLoad().Script(lineageMarkerSource(token)).Build()).Error())
			}

			b2 := newLineageTestBrokerPrefix(t, tt, false, b1.config.Prefix)
			after := lineagePublish(t, b2, ch, 1)
			require.Equal(t, before.Epoch, after.Epoch)
			require.Equal(t, uint64(3), after.Offset)
		})
	}
}

// sentinelSetupPorts finds the primary and a replica of the Sentinel setup
// (ports 6380-6382).
func sentinelSetupPorts(t *testing.T) (primary, replica int) {
	t.Helper()
	for _, port := range []int{6380, 6381, 6382} {
		c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:" + strconv.Itoa(port)}, DisableCache: true})
		if err != nil {
			continue
		}
		hello, err := c.Do(context.Background(), c.B().Hello().Build()).AsMap()
		c.Close()
		if err != nil {
			continue
		}
		if parseHelloReply(helloFields(hello)).primary {
			if primary == 0 {
				primary = port
			}
		} else if replica == 0 {
			replica = port
		}
	}
	if primary == 0 || replica == 0 {
		t.Skip("no Sentinel setup with a primary and a replica")
	}
	return primary, replica
}

// The gate relies on a replica never holding the primary's scripts. Pinned for
// the Redis version in CI: the Sentinel setup has a primary on 6380 and
// replicas on 6381, 6382.
func TestRedisLineage_ReplicaHasNoScripts(t *testing.T) {
	pPort, rPort := sentinelSetupPorts(t)
	primary, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:" + strconv.Itoa(pPort)}, DisableCache: true})
	require.NoError(t, err)
	defer primary.Close()
	replica, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:" + strconv.Itoa(rPort)}, DisableCache: true})
	require.NoError(t, err)
	defer replica.Close()

	ctx := context.Background()
	hello, err := primary.Do(ctx, primary.B().Hello().Build()).AsMap()
	require.NoError(t, err)
	if !parseHelloReply(helloFields(hello)).gateCapable {
		t.Skip("server can't use the gate")
	}

	src := "return 'lineage-replica-probe-" + randString(8) + "'"
	sha, err := primary.Do(ctx, primary.B().ScriptLoad().Script(src).Build()).ToString()
	require.NoError(t, err)
	_, err = primary.Do(ctx, primary.B().Eval().Script(src+" -- eval").Numkeys(0).Build()).ToString()
	require.NoError(t, err)
	// The server computes the digest: SHA-1 is unavailable here in FIPS-only mode.
	evalSha, err := primary.Do(ctx, primary.B().Eval().Script(lineageSha1Source).Numkeys(0).Arg(src+" -- eval").Build()).ToString()
	require.NoError(t, err)
	// Replication is asynchronous: let it catch up with a write first.
	probeKey := "lineage-probe-" + randString(8)
	require.NoError(t, primary.Do(ctx, primary.B().Set().Key(probeKey).Value("1").Ex(time.Minute).Build()).Error())
	require.Eventually(t, func() bool {
		v, err := replica.Do(ctx, replica.B().Get().Key(probeKey).Build()).ToString()
		return err == nil && v == "1"
	}, 5*time.Second, 50*time.Millisecond)

	exists, err := replica.Do(ctx, replica.B().ScriptExists().Sha1(sha, evalSha).Build()).AsIntSlice()
	require.NoError(t, err)
	require.Equal(t, []int64{0, 0}, exists, "a replica must never hold the primary's scripts, or the lineage gate can't see promotions")
}

// Another node rotated the token: this node's scripts get a LINEAGE error and
// it follows the server's token without rotating again.
func TestRedisLineage_FollowsTokenOfAnotherNode(t *testing.T) {
	for _, tt := range lineageRedisTests {
		if !tt.Gate {
			continue
		}
		t.Run(tt.Name, func(t *testing.T) {
			b1 := newLineageTestBroker(t, tt, false)
			ch := "lineage_follow_" + randString(6)
			lineagePublish(t, b1, ch, 1)

			b2 := newLineageTestBrokerPrefix(t, tt, false, b1.config.Prefix)
			s2 := b2.shards[0].shard
			flushScripts(t, s2)
			// b2 opens the gate first: a rotation and a new epoch.
			after := lineagePublish(t, b2, ch, 1)
			require.Equal(t, uint64(1), after.Offset)

			s1 := b1.shards[0].shard
			before := b1.lin(s1).stats.rotations
			// b1 still knows the old token: LINEAGE error, then the new token's scripts.
			sp := lineagePublish(t, b1, ch, 1)
			require.Equal(t, after.Epoch, sp.Epoch)
			require.Equal(t, uint64(2), sp.Offset)
			require.Equal(t, before, b1.lin(s1).stats.rotations, "following a token is not a rotation")
		})
	}
}

// A server that isn't a primary never gets our scripts loaded (a demoted
// server keeps its script cache): the gate refuses.
func TestRedisLineage_GateRefusesReplica(t *testing.T) {
	_, rPort := sentinelSetupPorts(t)
	node := testNode(t)
	s, err := NewRedisShard(node, testSingleRedisConf(rPort)) // a replica of the Sentinel setup
	require.NoError(t, err)
	defer s.Close()
	hello, err := s.client.Do(context.Background(), s.client.B().Hello().Build()).AsMap()
	require.NoError(t, err)
	kind := parseHelloReply(helloFields(hello))
	if kind.primary || !kind.gateCapable {
		t.Skip("the replica is not a gate-capable server")
	}
	l := s.lineageFor("lineage_test")
	err = l.open(context.Background(), l.keyForSlot(0), false, "", "")
	require.ErrorIs(t, err, errLineageGate)
	require.Contains(t, err.Error(), "not a primary")
}

// A slot migrated in from another server carries that server's token (its
// token key moves with it). The gate adopts it under this server's token,
// keeping its streams, instead of resetting every slot of the server.
func TestRedisLineage_MigratedSlotAdopted(t *testing.T) {
	for _, tt := range lineageRedisTests {
		if !tt.Gate || !tt.UseCluster {
			continue
		}
		t.Run(tt.Name, func(t *testing.T) {
			b := newLineageTestBroker(t, tt, false)
			s := b.shards[0].shard
			ctx := context.Background()
			ch := "lineage_migrated_" + randString(6)
			other := "lineage_migrated_other_" + randString(6)
			before := lineagePublish(t, b, ch, 2)
			otherBefore := lineagePublish(t, b, other, 1)

			// Emulate the migration: the slot's token key and the meta carry a
			// foreign token.
			metaKey := string(b.historyMetaKey(s, ch))
			tokenKey := b.lin(s).tokenKey(metaKey)
			require.NoError(t, s.client.Do(ctx, s.client.B().Hset().Key(tokenKey).FieldValue().FieldValue("t", "gforeign").Build()).Error())
			require.NoError(t, s.client.Do(ctx, s.client.B().Hset().Key(metaKey).FieldValue().FieldValue("i", "gforeign").Build()).Error())

			after := lineagePublish(t, b, ch, 1)
			require.Equal(t, before.Epoch, after.Epoch, "a migrated slot keeps its streams")
			require.Equal(t, uint64(3), after.Offset)
			value, err := s.client.Do(ctx, s.client.B().Hget().Key(tokenKey).Field("t").Build()).ToString()
			require.NoError(t, err)
			tok, anc := parseTokenValue(value)
			require.NotEqual(t, "gforeign", tok)
			require.Equal(t, "gforeign", anc, "the foreign token is the ancestor")

			otherAfter := lineagePublish(t, b, other, 1)
			if redisKeySlot(string(b.historyMetaKey(s, other))) != redisKeySlot(metaKey) {
				require.Equal(t, otherBefore.Epoch, otherAfter.Epoch, "other slots of the server are untouched")
			}
		})
	}
}
