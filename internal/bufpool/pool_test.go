package bufpool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferPool(t *testing.T) {
	buf := GetBuffer()
	buf.Write([]byte("1"))
	require.Equal(t, "1", buf.String())
	PutBuffer(buf)
	buf = GetBuffer()
	buf.Write([]byte("2"))
	require.Equal(t, "2", buf.String())
}
