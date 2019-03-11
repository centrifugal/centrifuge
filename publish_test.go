package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithUID(t *testing.T) {
	opt := SkipHistory()
	opts := &PublishOptions{}
	opt(opts)
	assert.Equal(t, true, opts.SkipHistory)
}
