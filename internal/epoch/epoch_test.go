package epoch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerate(t *testing.T) {
	e := Generate()
	require.Len(t, e, 8)
	for _, c := range e {
		require.Contains(t, letters, string(c))
	}
}

func TestGenerateUniqueness(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		e := Generate()
		_, exists := seen[e]
		require.False(t, exists, "duplicate epoch generated: %s", e)
		seen[e] = struct{}{}
	}
}
