package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnsubscribe_String(t *testing.T) {
	t.Parallel()
	require.Equal(t, `code: 0, reason: client unsubscribed`, unsubscribeClient.String())
}
