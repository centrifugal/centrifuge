package centrifuge

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

type holdMapPresenceBroker struct {
	*MemoryMapBroker
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (b *holdMapPresenceBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error) {
	if strings.HasPrefix(ch, "cp:") && b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return b.MemoryMapBroker.Publish(ctx, ch, key, opts)
}

// A presence tick adds presence for a subscription with EmitPresence and map
// client presence. While its map presence add is in flight the subscription is
// unsubscribed and the channel subscribed again with EmitPresence only. The
// tick's compensation removes only the map client presence: the channel presence
// belongs to the new subscription.
func TestOrderTickCompensationRemovesLivePresence(t *testing.T) {
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(LogEntry) {},
		Map: MapConfig{GetMapChannelOptions: func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Minute}
		}},
	})
	require.NoError(t, err)
	mapBroker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	hb := &holdMapPresenceBroker{MemoryMapBroker: mapBroker, entered: make(chan struct{}), release: make(chan struct{})}
	node.SetMapBroker(hb)
	node.OnConnect(func(client *Client) {})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	client, _, err := NewClient(SetCredentials(ctx, &Credentials{UserID: "u"}), node, transport)
	require.NoError(t, err)
	require.True(t, client.HandleCommand(&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}}, 0))

	const ch = "tick_compensation"
	withMapPresence := func(o *SubscribeOptions) { o.MapClientPresenceChannel = "cp:" + ch }
	require.NoError(t, client.Subscribe(ch, WithEmitPresence(true), withMapPresence))

	hb.armed.Store(true)
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		client.updatePresence()
	}()
	<-hb.entered // The tick added channel presence, its map presence add is in flight.

	client.Unsubscribe(ch)
	require.NoError(t, client.Subscribe(ch, WithEmitPresence(true)))
	presence, err := node.Presence(ch)
	require.NoError(t, err)
	require.Contains(t, presence.Presence, client.ID(), "the new subscription added its presence")

	close(hb.release)
	<-tickDone
	presence, err = node.Presence(ch)
	require.NoError(t, err)
	require.Contains(t, presence.Presence, client.ID(), "presence of the live subscription removed by the tick's compensation")
}
