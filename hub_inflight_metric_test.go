package centrifuge

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestSubscriptionsInflight_NoDriftOnResubscribeOverwrite guards the
// subscriptions_inflight gauge against drift. It must track numSubs exactly:
// incremented only when a genuinely new client+channel subscription is added,
// decremented only when one is actually removed. A resubscribe whose addSub
// overwrites an existing entry (a fresh generation before the prior
// unsubscribe's removeSub ran) adds no new subscription, and the stale
// generation's removeSub is a no-op — so the gauge must not move for either.
func TestSubscriptionsInflight_NoDriftOnResubscribeOverwrite(t *testing.T) {
	registry := prometheus.NewRegistry()
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(LogEntry) {},
		Metrics:    MetricsConfig{RegistererGatherer: registry},
	})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "u")
	const ch = "inflight-ch"

	inflight := func() float64 {
		mfs, gErr := registry.Gather()
		require.NoError(t, gErr)
		var sum float64
		for _, mf := range mfs {
			if strings.Contains(mf.GetName(), "subscriptions_inflight") {
				for _, m := range mf.GetMetric() {
					sum += m.GetGauge().GetValue()
				}
			}
		}
		return sum
	}

	// Initial subscription (generation 5).
	_, err = node.addSubscription(ch, subInfo{client: client, subGen: 5})
	require.NoError(t, err)
	require.Equal(t, float64(1), inflight())

	// Resubscribe overwrite: a fresh generation for the SAME client+channel, as
	// happens when a resubscribe's addSub runs before the prior unsubscribe's
	// removeSub. No new subscription — inflight must stay 1.
	_, err = node.addSubscription(ch, subInfo{client: client, subGen: 7})
	require.NoError(t, err)
	require.Equal(t, float64(1), inflight(), "inflight drifted on resubscribe overwrite")

	// Stale unsubscribe (generation 5): gen mismatch, nothing removed — stays 1.
	require.NoError(t, node.removeSubscription(ch, client, 5))
	require.Equal(t, float64(1), inflight(), "stale unsubscribe wrongly changed inflight")

	// Real unsubscribe (generation 7): back to 0.
	require.NoError(t, node.removeSubscription(ch, client, 7))
	require.Equal(t, float64(0), inflight())
}
