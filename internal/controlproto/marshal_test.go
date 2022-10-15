package controlproto

import (
	"testing"

	"github.com/centrifugal/centrifuge/internal/controlpb"

	"github.com/stretchr/testify/require"
)

func TestEncoder(t *testing.T) {
	encoder := NewProtobufEncoder()

	cmd := &controlpb.Command{
		Uid:        "test",
		Disconnect: &controlpb.Disconnect{},
	}
	d, err := encoder.EncodeCommand(cmd)
	require.NoError(t, err)
	require.NotNil(t, d)
}
