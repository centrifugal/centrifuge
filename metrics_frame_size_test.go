package centrifuge

import (
	"bytes"
	"context"
	"testing"

	"github.com/centrifugal/protocol"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// frameSizeStats reads the histogram's count and sum for a transport.
func frameSizeStats(t *testing.T, n *Node, transport string) (count uint64, sum float64) {
	t.Helper()
	obs, err := n.metrics.transportFrameSizeHistogram.GetMetricWithLabelValues(transport)
	require.NoError(t, err)
	var m dto.Metric
	require.NoError(t, obs.(prometheus.Metric).Write(&m))
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

func frameSizeTestNode(t *testing.T) *Node {
	t.Helper()
	n, err := New(Config{Metrics: MetricsConfig{
		RegistererGatherer: prometheus.NewRegistry(),
	}})
	require.NoError(t, err)
	require.NoError(t, n.Run())
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })
	n.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "u"}}, nil
	})
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})
	return n
}

func encodeFrame(t *testing.T, cmds ...*protocol.Command) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := protocol.NewJSONCommandEncoder()
	for _, cmd := range cmds {
		data, err := enc.Encode(cmd)
		require.NoError(t, err)
		buf.Write(data)
	}
	return buf.Bytes()
}

// TestFrameSizeMetricMeasuresFramesNotCommands is the reason this metric exists.
//
// messages_received and messages_received_size already count commands, so a
// frame metric is only worth having if a frame is not a command. It is not: the
// protocol batches, and the SDKs use it - centrifuge-js puts the connect command
// and every subscribe into one frame on each transport open. The transport read
// limit bounds that whole frame, so the frame is the thing an operator has to
// size against, and dividing the existing counters gives a mean command size
// that says nothing about it.
func TestFrameSizeMetricMeasuresFramesNotCommands(t *testing.T) {
	n := frameSizeTestNode(t)
	transport := newTestTransport(func() {})
	client, closeFn, err := NewClient(context.Background(), n, transport)
	require.NoError(t, err)
	defer func() { _ = closeFn() }()

	// One frame carrying connect plus three subscribes - the reconnect shape.
	frame := encodeFrame(t,
		&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}},
		&protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: "a"}},
		&protocol.Command{Id: 3, Subscribe: &protocol.SubscribeRequest{Channel: "b"}},
		&protocol.Command{Id: 4, Subscribe: &protocol.SubscribeRequest{Channel: "c"}},
	)
	require.True(t, HandleReadFrame(client, bytes.NewReader(frame), 1<<20))

	count, sum := frameSizeStats(t, n, transport.Name())
	require.Equal(t, uint64(1), count, "four commands in one frame must be one observation")
	require.InDelta(t, float64(len(frame)), sum, float64(len(frame))*0.1,
		"the observation must be the whole frame, not one command")
}

// TestFrameSizeMetricCountsEveryFrame pins that the _count series is the frame
// count, which is what makes commands-per-frame derivable without a second
// metric: messages_received / frame_size_count.
func TestFrameSizeMetricCountsEveryFrame(t *testing.T) {
	n := frameSizeTestNode(t)
	transport := newTestTransport(func() {})
	client, closeFn, err := NewClient(context.Background(), n, transport)
	require.NoError(t, err)
	defer func() { _ = closeFn() }()

	require.True(t, HandleReadFrame(client,
		bytes.NewReader(encodeFrame(t, &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}})), 1<<20))
	for i := 0; i < 4; i++ {
		require.True(t, HandleReadFrame(client,
			bytes.NewReader(encodeFrame(t, &protocol.Command{
				Id: uint32(i + 2), Subscribe: &protocol.SubscribeRequest{Channel: "ch"},
			})), 1<<20))
	}

	count, _ := frameSizeStats(t, n, transport.Name())
	require.Equal(t, uint64(5), count, "one observation per frame")
}
