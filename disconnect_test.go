package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDisconnect_CloseText(t *testing.T) {
	d := DisconnectForceReconnect
	closeText := d.CloseText()
	require.Equal(t, `{"reason":"force reconnect","reconnect":true}`, closeText)
	closeText = d.CloseText()
	require.Equal(t, `{"reason":"force reconnect","reconnect":true}`, closeText)
	d = DisconnectForceNoReconnect
	closeText = d.CloseText()
	require.Equal(t, `{"reason":"force disconnect","reconnect":false}`, closeText)
}

var benchCloseTextString string

func BenchmarkDisconnect_CloseText(b *testing.B) {
	d := DisconnectForceReconnect
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchCloseTextString = d.CloseText()
	}
}
