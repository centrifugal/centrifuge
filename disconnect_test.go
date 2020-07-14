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

func TestDisconnect_String(t *testing.T) {
	d := Disconnect{
		Code:            42,
		Reason:          "reason",
		Reconnect:       true,
		cachedCloseText: "some information",
	}
	stringText := d.String()
	require.Equal(t, "code: 42, reason: reason, reconnect: true", stringText)
}

func TestDisconnect_Error(t *testing.T) {
	d := Disconnect{
		Code:            42,
		Reason:          "reason",
		Reconnect:       true,
		cachedCloseText: "some information",
	}
	errorText := d.Error()
	require.Equal(t, "disconnected: code: 42, reason: reason, reconnect: true", errorText)
}
