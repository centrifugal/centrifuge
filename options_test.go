package centrifuge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithHistory(t *testing.T) {
	opt := WithHistory(10, time.Second)
	opts := &PublishOptions{}
	opt(opts)
	assert.Equal(t, 10, opts.HistorySize)
	assert.Equal(t, time.Second, opts.HistoryTTL)
}

func TestSkipHistory(t *testing.T) {
	opt := SkipHistory()
	opts := &PublishOptions{}
	opt(opts)
	assert.Equal(t, true, opts.skipHistory)
}

func TestWithResubscribe(t *testing.T) {
	opt := WithResubscribe()
	opts := &UnsubscribeOptions{}
	opt(opts)
	assert.Equal(t, true, opts.Resubscribe)
}

func TestWithReconnect(t *testing.T) {
	opt := WithReconnect()
	opts := &DisconnectOptions{}
	opt(opts)
	assert.Equal(t, true, opts.Reconnect)
}
