package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipHistory(t *testing.T) {
	opt := SkipHistory()
	opts := &PublishOptions{}
	opt(opts)
	assert.Equal(t, true, opts.SkipHistory)
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
