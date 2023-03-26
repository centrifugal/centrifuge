package controlproto

import (
	"testing"

	"github.com/centrifugal/centrifuge/internal/controlpb"

	"github.com/stretchr/testify/require"
)

func TestDecoder(t *testing.T) {
	encoder := NewProtobufEncoder()
	decoder := NewProtobufDecoder()

	cmd := &controlpb.Command{
		Uid:        "test",
		Disconnect: &controlpb.Disconnect{},
	}
	d, err := encoder.EncodeCommand(cmd)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedCmd, err := decoder.DecodeCommand(d)
	require.NoError(t, err)
	require.Equal(t, cmd, decodedCmd)
}
