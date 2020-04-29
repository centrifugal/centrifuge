package cancelctx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCustomContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	customCtx := New(ctx, ch)
	require.NoError(t, customCtx.Err())
	dlTime, dlSet := customCtx.Deadline()
	require.Zero(t, dlTime)
	require.False(t, dlSet)
	select {
	case <-customCtx.Done():
		require.Fail(t, "must not be cancelled")
	default:
	}
	cancel()
	select {
	case <-customCtx.Done():
		require.Fail(t, "must not be cancelled")
	default:
	}
	close(ch)
	select {
	case <-customCtx.Done():
		require.Equal(t, context.Canceled, customCtx.Err())
	default:
		require.Fail(t, "must be cancelled")
	}
}
