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
	opt := WithResubscribe()
	opts := &UnsubscribeOptions{}
	opt(opts)
	require.Equal(t, true, opts.Resubscribe)
}

func TestWithReconnect(t *testing.T) {
	opt := WithReconnect()
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, true, opts.Reconnect)
}
