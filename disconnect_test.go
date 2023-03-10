package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, "code: 42, reason: reason", errorText)
}
