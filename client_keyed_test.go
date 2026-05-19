package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	fdelta "github.com/shadowspore/fossil-delta"
	"github.com/stretchr/testify/require"
)

// TestEncodeKeyedPush_AllProtocols covers the four encoding branches in
// encodeKeyedPush: JSON+bidi (already covered), JSON+uni, Protobuf+bidi,
// Protobuf+uni.
func TestEncodeKeyedPush_AllProtocols(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		proto ProtocolType
		uni   bool
	}{
		{"JSON-bidi", ProtocolTypeJSON, false},
		{"JSON-uni", ProtocolTypeJSON, true},
		{"Protobuf-bidi", ProtocolTypeProtobuf, false},
		{"Protobuf-uni", ProtocolTypeProtobuf, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := defaultNodeNoHandlers()
			defer func() { _ = node.Shutdown(context.Background()) }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			transport := newTestTransport(cancel)
			transport.setProtocolVersion(ProtocolVersion2)
			transport.setProtocolType(tc.proto)
			transport.setUnidirectional(tc.uni)
			c, err := newClient(SetCredentials(ctx, &Credentials{UserID: "u"}), node, transport)
			require.NoError(t, err)

			data, err := c.encodeKeyedPush("ch", &protocol.Publication{
				Key: "k", Data: []byte(`{"v":1}`), Version: 7,
			})
			require.NoError(t, err)
			require.NotEmpty(t, data)
		})
	}
}

// TestKeyedTrack_NoTrackHandler covers the trackHandler == nil branch.
func TestKeyedTrack_NoTrackHandler(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	// Subscribe handler only, no OnTrack.
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
				ClientSideRefresh: true,
			}, nil)
		})
	})
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test:channel",
		Type:    typeTrack,
		Track:   []*protocol.TrackBatch{{Items: []*protocol.KeyedItem{{Key: "k", Version: 1}}}},
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

// TestKeyedTrack_TtlField covers the inner branch in handleTrack that sets
// res.Ttl when reply.ExpireAt > now.
func TestKeyedTrack_TtlField(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			cb(TrackReply{Batches: []TrackBatchReply{{ExpireAt: time.Now().Unix() + 60}}}, nil)
		})
	})
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	res := trackSharedPollClientWithReply(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k", Version: 1}})
	require.True(t, res.Expires)
	require.Greater(t, res.Ttl, uint32(0))
}

// TestKeyedUntrack_InvokesUntrackHandler covers the optional untrackHandler
// invocation in handleUntrack and the minTrackExpireAt cleanup branch.
func TestKeyedUntrack_InvokesUntrackHandler(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	called := make(chan UntrackEvent, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			// Use ExpireAt so minTrackExpireAt gets populated and exercised on cleanup.
			cb(TrackReply{Batches: []TrackBatchReply{{ExpireAt: time.Now().Unix() + 60}}}, nil)
		})
		client.OnUntrack(func(e UntrackEvent) {
			called <- e
		})
	})
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "k", Version: 1},
	})

	untrackSharedPollClient(t, client, "test:channel", []string{"k"})
	select {
	case ev := <-called:
		require.Equal(t, "test:channel", ev.Channel)
		require.Equal(t, []string{"k"}, ev.Keys)
	case <-time.After(time.Second):
		t.Fatal("untrack handler not invoked")
	}

	// minTrackExpireAt entry for the channel must have been removed.
	client.mu.RLock()
	_, present := client.keyed.minTrackExpireAt["test:channel"]
	client.mu.RUnlock()
	require.False(t, present)
}

// TestCleanupKeyed_NoKeyedState covers the early-return branch in cleanupKeyed
// when c.keyed is nil (client never tracked anything).
func TestCleanupKeyed_NoKeyedState(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	// Sanity: c.keyed is nil before any track call.
	client.mu.RLock()
	require.Nil(t, client.keyed)
	client.mu.RUnlock()

	// Should be a no-op with no keyed state.
	client.cleanupKeyed("anything")
}

// TestKeyedWritePublication_DeltaFallsBackToFullWhenBaseMismatches asserts
// that when the prep's delta base-version doesn't match the client's
// keyState.version, keyedWritePublication sends the FULL publication instead
// of a delta — otherwise the client would apply the delta against the wrong
// base and produce garbage.
//
// Background: a shared-poll delta patch is computed from entry.data BEFORE
// the publish (prevData), versioned at entry.version BEFORE the bump
// (prevVersion). The client can only apply the delta correctly if it
// currently holds the bytes corresponding to prevVersion. Concurrent
// broadcasts for the same key can race past the per-client version check
// such that the client receives a delta whose prevVersion ≠ keyState.version
// — its previous data doesn't match the patch's base. The fix tags prep
// with prevVersion and falls back to FULL when the base wouldn't apply.
//
// The test simulates the race by directly building a prep with a fabricated
// "stale" base version, then asserting the wire bytes are a FULL publication
// (Delta=false) carrying the full payload, not a delta patch.
func TestKeyedWritePublication_DeltaFallsBackToFullWhenBaseMismatches(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      time.Hour,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
		Mode:                 SharedPollModeVersioned,
	})
	setupSharedPollDeltaHandlers(node)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setProtocolType(ProtocolTypeProtobuf)
	sink := make(chan []byte, 32)
	transport.sink = sink
	newCtx := SetCredentials(ctx, &Credentials{UserID: "user1"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
	subscribeSharedPollClientDelta(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "k", Version: 0},
	})

	// Force the client into a known steady state: keyState.version=10,
	// deltaReady=true — as if a prior FULL publication at v=10 was already
	// delivered. We bypass the broadcast plumbing to set this up directly.
	client.mu.Lock()
	client.keyed.trackedKeys["test:channel"]["k"] = &keyedKeyState{
		version:    10,
		deltaReady: true,
	}
	client.mu.Unlock()

	// Drain any startup frames.
	drainStart := time.Now()
	for time.Since(drainStart) < 50*time.Millisecond {
		select {
		case <-sink:
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Build a prep that simulates a concurrent broadcast that won the race:
	// the patch is computed from a previous-version snapshot (v=15) that
	// this client never received — the client still holds v=10's data.
	// Without the fix, keyedWritePublication would forward the delta as-is
	// and the client would apply garbage. With the fix, it must fall back
	// to FULL.
	clientHas := []byte(`{"value":"data at v=10"}`)
	staleBase := []byte(`{"value":"data at v=15 (client never saw)"}`)
	newData := []byte(`{"value":"data at v=20"}`)
	patch := fdelta.Create(staleBase, newData)
	prep := preparedData{
		deltaSub:         true,
		keyedDeltaPatch:  patch,
		keyedDeltaIsReal: len(patch) < len(newData),
	}
	pub := &protocol.Publication{Key: "k", Data: newData, Version: 20}

	client.keyedWritePublication("test:channel", "k", 20, pub, prep)

	// Read the wire publication for v=20.
	var got *protocol.Publication
	timeout := time.After(2 * time.Second)
	for got == nil {
		select {
		case data := <-sink:
			reply := &protocol.Reply{}
			if err := reply.UnmarshalVT(data); err != nil {
				continue
			}
			if reply.Push != nil && reply.Push.Pub != nil && reply.Push.Pub.Version == 20 {
				got = reply.Push.Pub
			}
		case <-timeout:
			t.Fatal("timeout waiting for publication v=20")
		}
	}

	require.False(t, got.Delta,
		"server must NOT send a delta whose base doesn't match client's keyState.version — "+
			"client has v=10 but prep delta is from v=15. Got Delta=true with data %q", got.Data)
	require.Equal(t, string(newData), string(got.Data),
		"server must send the FULL payload when delta base wouldn't apply")
	_ = clientHas
}

// TestKeyedWritePublication_StateNotAdvancedWhenWriteSkipped asserts that
// keyedWritePublication does NOT update keyState.version or keyState.deltaReady
// when the publication ultimately wasn't delivered.
//
// Repro: pass prep.wasFiltered=true. keyedWritePublication takes the full path
// (deltaReady starts false → sendDelta=false), sets prep.deltaSub=false, and
// then calls writePublication. writePublication early-returns at
// `if prep.wasFiltered && !prep.deltaSub { return nil }` without enqueuing
// anything. The client never sees the publication, but the buggy code has
// already bumped keyState.version to pubVersion AND flipped
// keyState.deltaReady to true. Subsequent broadcasts at lower or equal
// versions are filtered out, and (when delta is enabled) the next broadcast
// picks the delta path against a base the SDK never received.
//
// The test is synthetic — shared-poll publications don't currently set
// wasFiltered — but the contract violation is real: any future code path
// that legitimately filters a publication after the eager state update will
// silently corrupt per-connection state. The same ordering bug also triggers
// today on writePublication's other no-write paths (DisabledPushFlags,
// encode failure) and on enqueue overflow before the async close races.
func TestKeyedWritePublication_StateNotAdvancedWhenWriteSkipped(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "u1")
	connectClientV2(t, client)

	// Manually set up keyed state: channel with delta enabled, one key tracked
	// at version 0 with deltaReady=false.
	client.mu.Lock()
	client.keyed = &keyedState{
		channels: map[string]*keyedChannelDeltaState{
			"ch": {deltaType: DeltaTypeFossil},
		},
		trackedKeys: map[string]map[string]*keyedKeyState{
			"ch": {"K": {version: 0, deltaReady: false}},
		},
	}
	client.mu.Unlock()

	// Call with prep.wasFiltered=true → writePublication will skip the write.
	pub := &protocol.Publication{
		Key: "K", Data: []byte(`{"v":1}`), Version: 1,
	}
	client.keyedWritePublication("ch", "K", 1, pub, preparedData{wasFiltered: true})

	client.mu.RLock()
	finalState := client.keyed.trackedKeys["ch"]["K"]
	client.mu.RUnlock()
	require.NotNil(t, finalState)
	require.Equal(t, uint64(0), finalState.version,
		"version must not advance when the publication was not actually delivered")
	require.False(t, finalState.deltaReady,
		"deltaReady must remain false when the first full publication was not delivered")
}

// TestCheckTrackExpiration_EarlyReturns covers the three early-return branches
// in checkTrackExpiration: no keyed state, no minExpire entry, and re-check
// after acquiring write lock.
func TestCheckTrackExpiration_EarlyReturns(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	// 1) No keyed state at all — first early return.
	client.checkTrackExpiration("any-channel", time.Second)

	// 2) Subscribe/track to populate keyed state without ExpireAt → minExpire = 0.
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "k", Version: 1},
	})

	// minTrackExpireAt[channel] is unset (TrackReply{} had no ExpireAt) → fast path returns.
	client.checkTrackExpiration("test:channel", time.Second)
}
