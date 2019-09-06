package centrifuge

import (
	"context"
	"time"
)

type customCancelContext struct {
	ctx context.Context
	ch  <-chan struct{}
}

func (c customCancelContext) Deadline() (time.Time, bool)       { return time.Time{}, false }
func (c customCancelContext) Done() <-chan struct{}             { return c.ch }
func (c customCancelContext) Err() error                        { return nil }
func (c customCancelContext) Value(key interface{}) interface{} { return c.ctx.Value(key) }

// newCustomCancelContext returns a context that will be canceled on channel close.
func newCustomCancelContext(ctx context.Context, ch <-chan struct{}) context.Context {
	return customCancelContext{ctx: ctx, ch: ch}
}
