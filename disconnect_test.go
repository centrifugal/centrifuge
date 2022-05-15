package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDisconnect_CloseText(t *testing.T) {
	d := DisconnectForceReconnect
	closeText := d.CloseText(ProtocolVersion1)
	require.Equal(t, `{"reason":"force reconnect","reconnect":true}`, closeText)
	closeText = d.CloseText(ProtocolVersion1)
	require.Equal(t, `{"reason":"force reconnect","reconnect":true}`, closeText)
	d = DisconnectForceNoReconnect
	closeText = d.CloseText(ProtocolVersion1)
	require.Equal(t, `{"reason":"force disconnect","reconnect":false}`, closeText)
	closeText = d.CloseText(ProtocolVersion2)
	require.Equal(t, `force disconnect`, closeText)
}

func TestDisconnect_String(t *testing.T) {
	d := Disconnect{
		Code:   42,
		Reason: "reason",
	}
	stringText := d.String()
	require.Equal(t, "code: 42, reason: reason", stringText)
}

func TestDisconnect_Error(t *testing.T) {
	d := Disconnect{
		Code:   42,
		Reason: "reason",
	}
	errorText := d.Error()
	require.Equal(t, "disconnected: code: 42, reason: reason", errorText)
}
