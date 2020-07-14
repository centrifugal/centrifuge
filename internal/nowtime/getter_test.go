package nowtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	nowTime := Get()
	require.NotNil(t, nowTime)
	require.GreaterOrEqual(t, time.Now().Unix(), nowTime.Unix())
}
