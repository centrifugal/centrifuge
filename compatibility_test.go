package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompatibilityFlagExists(t *testing.T) {
	flags := UseSeqGen
	require.True(t, hasFlag(flags, UseSeqGen))
}

func TestCompatibilityFlagNotExists(t *testing.T) {
	var flags uint64
	require.False(t, hasFlag(flags, UseSeqGen))
}
