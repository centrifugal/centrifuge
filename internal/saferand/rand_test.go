package saferand

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRand_Int63n(t *testing.T) {
	r := New(time.Now().Unix())
	require.True(t, r.Int63n(200)+1 > 0)
}

func TestRand_Intn(t *testing.T) {
	r := New(time.Now().Unix())
	require.True(t, r.Intn(200)+1 > 0)
}
