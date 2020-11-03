package centrifuge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithHistory(t *testing.T) {
	opt := WithHistory(10, time.Second)
	opts := &PublishOptions{}
	opt(opts)
	require.Equal(t, 10, opts.HistorySize)
	require.Equal(t, time.Second, opts.HistoryTTL)
}

func TestWithResubscribe(t *testing.T) {
	opt := WithResubscribe(true)
	opts := &UnsubscribeOptions{}
	opt(opts)
	require.Equal(t, true, opts.Resubscribe)
}

func TestWithDisconnect(t *testing.T) {
	opt := WithDisconnect(DisconnectConnectionLimit)
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, DisconnectConnectionLimit, opts.Disconnect)
}

func TestWithClientWhitelist(t *testing.T) {
	opt := WithClientWhitelist([]string{"client"})
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, []string{"client"}, opts.ClientWhitelist)
}

func TestWithLimit(t *testing.T) {
	opt := WithLimit(NoLimit)
	opts := &HistoryOptions{}
	opt(opts)
	require.Equal(t, NoLimit, opts.Limit)
}
