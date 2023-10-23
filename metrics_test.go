package centrifuge

import (
	"strconv"
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func BenchmarkTransportMessagesSent(b *testing.B) {
	m, err := initMetricsRegistry(prometheus.DefaultRegisterer, "test")
	require.NoError(b, err)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incTransportMessagesSent("test", protocol.FrameTypePushPublication, "channel"+strconv.Itoa(i%10), 200)
		}
	})
}

func BenchmarkTransportMessagesReceived(b *testing.B) {
	m, err := initMetricsRegistry(prometheus.DefaultRegisterer, "test")
	require.NoError(b, err)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incTransportMessagesReceived("test", protocol.FrameTypePushPublication, "channel"+strconv.Itoa(i%10), 200)
		}
	})
}
