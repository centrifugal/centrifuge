//go:build integration

package centrifuge

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// rewindStream makes the server look like a replica that missed the last
// publications: the meta offset goes back to keep and later entries are gone,
// with the same epoch. This is what a lossy failover leaves when nothing
// gives the stream a new epoch (no lineage detection: Redis 6, Dragonfly, a
// proxy hiding the servers, ...).
func rewindStream(t *testing.T, b *RedisBroker, ch string, keep int) {
	t.Helper()
	s := b.shards[0].shard
	rewindRedisStream(t, s, string(b.historyMetaKey(s, ch)), string(b.historyStreamKey(s, ch)), keep)
}

// rewindRedisStream leaves a stream as a replica that only got its first keep
// entries has it.
func rewindRedisStream(t *testing.T, s *RedisShard, metaKey, streamKey string, keep int) {
	t.Helper()
	ctx := context.Background()
	entries, err := s.client.Do(ctx, s.client.B().Xrange().Key(streamKey).Start("-").End(strconv.Itoa(keep)+"-0").Build()).AsXRange()
	require.NoError(t, err)
	require.NoError(t, s.client.Do(ctx, s.client.B().Del().Key(streamKey).Build()).Error())
	for _, e := range entries {
		cmd := s.client.B().Xadd().Key(streamKey).Id(e.ID).FieldValue()
		for k, v := range e.FieldValues {
			cmd = cmd.FieldValue(k, v)
		}
		require.NoError(t, s.client.Do(ctx, cmd.Build()).Error())
	}
	require.NoError(t, s.client.Do(ctx, s.client.B().Hset().Key(metaKey).FieldValue().FieldValue("s", strconv.Itoa(keep)).Build()).Error())
}

func newWatermarkTestNode(t *testing.T) (*Node, *RedisBroker, chan struct{}) {
	t.Helper()
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	insufficient := make(chan struct{}, 16)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true}}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			if e.Code == UnsubscribeCodeInsufficient {
				insufficient <- struct{}{}
			}
		})
	})
	b := newTestRedisBroker(t, node, true, false, 6379)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		stopRedisBroker(b)
	})
	return node, b, insufficient
}

// A live subscriber past the offsets the server gives out again resyncs, and
// recovering from its old position fails: it can't skip the new publications.
func TestRedisBroker_ReusedOffsetsDetected(t *testing.T) {
	node, b, insufficient := newWatermarkTestNode(t)
	ch := "watermark_" + randString(8)

	client := newTestConnectedClientV2(t, node, "42")
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Recover: true}, &protocol.Command{}, time.Now(), rw.rw))

	var last StreamPosition
	for i := 0; i < 3; i++ {
		res, err := node.Publish(ch, []byte(`{}`), WithHistory(10, time.Minute))
		require.NoError(t, err)
		last = res.StreamPosition
	}
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.channels[ch].streamPosition.Offset == 3
	}, 5*time.Second, 10*time.Millisecond)

	// Lossy failover: the server only has offset 1.
	rewindStream(t, b, ch, 1)
	res, err := node.Publish(ch, []byte(`{"new":2}`), WithHistory(10, time.Minute))
	require.NoError(t, err)
	require.Equal(t, uint64(2), res.StreamPosition.Offset, "the offset is given out again")
	require.Equal(t, last.Epoch, res.StreamPosition.Epoch, "in the same epoch")

	select {
	case <-insufficient:
	case <-time.After(5 * time.Second):
		t.Fatal("the subscriber must resync")
	}

	// The stream got reset: the old position can't recover.
	require.Eventually(t, func() bool {
		top, err := node.streamTop(ch, time.Hour)
		return err == nil && top.Epoch != last.Epoch
	}, 5*time.Second, 10*time.Millisecond, "a new epoch")
	_, err = node.recoverHistory(ch, last, time.Hour, 0)
	require.ErrorIs(t, err, ErrorUnrecoverablePosition)
}

// Without a loss, offsets only grow and nobody resyncs.
func TestRedisBroker_NoLossNoReset(t *testing.T) {
	node, _, insufficient := newWatermarkTestNode(t)
	ch := "watermark_ok_" + randString(8)
	client := newTestConnectedClientV2(t, node, "42")
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Recover: true}, &protocol.Command{}, time.Now(), rw.rw))

	var first StreamPosition
	for i := 0; i < 50; i++ {
		res, err := node.Publish(ch, []byte(`{}`), WithHistory(100, time.Minute))
		require.NoError(t, err)
		if i == 0 {
			first = res.StreamPosition
		}
	}
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.channels[ch].streamPosition.Offset == 50
	}, 5*time.Second, 10*time.Millisecond)
	select {
	case <-insufficient:
		t.Fatal("no resync without a loss")
	default:
	}
	top, err := node.streamTop(ch, time.Hour)
	require.NoError(t, err)
	require.Equal(t, first.Epoch, top.Epoch)
}

// Map subscriptions: the same, and the state is kept for the reload.
func TestRedisMapBroker_ReusedOffsetsDetected(t *testing.T) {
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	mb := redisMapBrokerFactory{}.make(t, node)
	node.SetMapBroker(mb)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	ctx := context.Background()
	ch := randomChannel("watermark_map")

	// A local subscriber: the node subscribes to the channel's PUB/SUB.
	client := newTestConnectedClientV2(t, node, "42")
	_, _, err = node.hub.addSub(ch, subInfo{client: client})
	require.NoError(t, err)
	require.NoError(t, mb.Subscribe(ch))

	var last MapUpdateResult
	for _, k := range []string{"a", "b", "c"} {
		last, err = mb.Publish(ctx, ch, k, MapPublishOptions{Data: []byte(k)})
		require.NoError(t, err)
	}
	s := mb.shards[0].shard
	require.Eventually(t, func() bool {
		ws := mb.watermarks.shard(ch)
		ws.mu.Lock()
		defer ws.mu.Unlock()
		return ws.m[ch].offset == 3
	}, 5*time.Second, 10*time.Millisecond)

	// Lossy failover: the server's change stream is back at offset 1.
	rewindRedisStream(t, s, mb.metaKey(s, ch), mb.streamKey(s, ch), 1)
	_, err = mb.Publish(ctx, ch, "d", MapPublishOptions{Data: []byte("d")})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st, err := mb.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
		return err == nil && st.Position.Epoch != last.Position.Epoch && len(st.Publications) == 4
	}, 5*time.Second, 10*time.Millisecond, "a new epoch, the state kept")
}

// Control: without detection the subscriber skips the new publication as a
// duplicate and keeps going, and recovery from its old position succeeds
// while the new publications at the reused offsets are never delivered.
func TestRedisBroker_ReusedOffsetsControl(t *testing.T) {
	node, b, insufficient := newWatermarkTestNode(t)
	b.watermarks = nil
	ch := "watermark_control_" + randString(8)

	client := newTestConnectedClientV2(t, node, "42")
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Recover: true}, &protocol.Command{}, time.Now(), rw.rw))
	var last StreamPosition
	for i := 0; i < 3; i++ {
		res, err := node.Publish(ch, []byte(`{}`), WithHistory(10, time.Minute))
		require.NoError(t, err)
		last = res.StreamPosition
	}
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.channels[ch].streamPosition.Offset == 3
	}, 5*time.Second, 10*time.Millisecond)

	rewindStream(t, b, ch, 1)
	for i := 0; i < 3; i++ { // new offsets 2, 3, 4
		_, err := node.Publish(ch, []byte(`{}`), WithHistory(10, time.Minute))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.channels[ch].streamPosition.Offset == 4
	}, 5*time.Second, 10*time.Millisecond, "the client moved on to offset 4, skipping the new 2 and 3")
	select {
	case <-insufficient:
		t.Fatal("no detection expected in the control")
	default:
	}
	res, err := node.recoverHistory(ch, last, time.Hour, 0)
	require.NoError(t, err, "recovery from the old position passes: the loss is silent")
	require.Len(t, res.Publications, 1)
}

// No false positives: many concurrent publishers on two nodes, both
// subscribed, standalone and Redis Cluster with and without sharded PUB/SUB.
// Without a loss no offset may ever look reused.
func TestRedisBroker_WatermarkNoFalsePositives(t *testing.T) {
	cases := []struct {
		name       string
		cluster    bool
		partitions int
	}{
		{"standalone", false, 0},
		{"cluster", true, 0},
		{"cluster_sharded", true, 9},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prefix := getUniquePrefix()
			var nodes []*Node
			var brokers []*RedisBroker
			for i := 0; i < 2; i++ {
				node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
				require.NoError(t, err)
				var conf RedisShardConfig
				if tc.cluster {
					conf = RedisShardConfig{ClusterAddresses: []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}, IOTimeout: 10 * time.Second}
				} else {
					conf = testSingleRedisConf(6379)
				}
				s, err := NewRedisShard(node, conf)
				require.NoError(t, err)
				b, err := NewRedisBroker(node, RedisBrokerConfig{Prefix: prefix, Shards: []*RedisShard{s}, NumShardedPubSubPartitions: tc.partitions})
				require.NoError(t, err)
				node.SetBroker(b)
				require.NoError(t, node.Run())
				t.Cleanup(func() {
					_ = node.Shutdown(context.Background())
					stopRedisBroker(b)
				})
				nodes = append(nodes, node)
				brokers = append(brokers, b)
			}
			const numChannels = 50
			const perPublisher = 100
			chs := make([]string, numChannels)
			for i := range chs {
				chs[i] = "wm_fp_" + randString(6) + "_" + strconv.Itoa(i)
			}
			for i, node := range nodes {
				client := newTestConnectedClientV2(t, node, "42")
				for _, ch := range chs {
					_, _, err := node.hub.addSub(ch, subInfo{client: client})
					require.NoError(t, err)
				}
				require.NoError(t, brokers[i].Subscribe(chs...))
			}
			errCh := make(chan error, len(nodes)*4)
			for _, node := range nodes {
				for p := 0; p < 4; p++ {
					go func(node *Node, p int) {
						for j := 0; j < perPublisher; j++ {
							ch := chs[(p*perPublisher+j)%numChannels]
							if _, err := node.Publish(ch, []byte(`{}`), WithHistory(10, time.Minute)); err != nil {
								errCh <- err
								return
							}
						}
						errCh <- nil
					}(node, p)
				}
			}
			for i := 0; i < len(nodes)*4; i++ {
				require.NoError(t, <-errCh)
			}
			total := uint64(len(nodes) * 4 * perPublisher / numChannels)
			for _, b := range brokers {
				require.Eventually(t, func() bool {
					for _, ch := range chs {
						ws := b.watermarks.shard(ch)
						ws.mu.Lock()
						wm := ws.m[ch]
						ws.mu.Unlock()
						if wm.offset != total {
							return false
						}
					}
					return true
				}, 10*time.Second, 20*time.Millisecond, "every node saw every publication")
				for _, ch := range chs {
					ws := b.watermarks.shard(ch)
					ws.mu.Lock()
					lost := ws.m[ch].lost
					ws.mu.Unlock()
					require.False(t, lost, "false positive on %s", ch)
				}
			}
		})
	}
}
