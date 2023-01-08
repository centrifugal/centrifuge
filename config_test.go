package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_ProtocolVersionEnabled(t *testing.T) {
	c := Config{
		EnabledProtocolVersions: []ProtocolVersion{},
	}
	require.True(t, c.ProtocolVersionEnabled(ProtocolVersion1))
	require.True(t, c.ProtocolVersionEnabled(ProtocolVersion2))

	c = Config{
		EnabledProtocolVersions: []ProtocolVersion{ProtocolVersion2},
	}
	require.False(t, c.ProtocolVersionEnabled(ProtocolVersion1))
	require.True(t, c.ProtocolVersionEnabled(ProtocolVersion2))
}
