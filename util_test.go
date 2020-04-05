package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringInSlice(t *testing.T) {
	require.True(t, stringInSlice("test", []string{"boom", "test"}))
	require.False(t, stringInSlice("test", []string{"boom", "testing"}))
}
