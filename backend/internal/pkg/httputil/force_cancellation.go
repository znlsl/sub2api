package httputil

import (
	"context"
	"time"
)

type forceCancellationKey struct{}

// WithForceCancellation carries a separate operator signal. Client disconnect
// may allow late billing reads; this signal always terminates them immediately.
func WithForceCancellation(ctx, force context.Context) context.Context {
	return context.WithValue(ctx, forceCancellationKey{}, force)
}

// ForceCancellationCause remains observable even if the client cancelled first.
func ForceCancellationCause(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if force, ok := ctx.Value(forceCancellationKey{}).(context.Context); ok {
		return context.Cause(force)
	}
	return nil
}

// AfterForceCancellation watches the operator signal independently of a client
// cancellation that may have happened earlier. No goroutine waits while idle.
func AfterForceCancellation(ctx context.Context, f func()) func() bool {
	if ctx != nil {
		if force, ok := ctx.Value(forceCancellationKey{}).(context.Context); ok {
			return context.AfterFunc(force, f)
		}
	}
	return func() bool { return false }
}

// forceDetachedContext preserves values and only the independent force signal.
// No goroutine or early-release timer is attached while constructing requests.
type forceDetachedContext struct {
	context.Context
	force context.Context
}

func (c forceDetachedContext) Deadline() (time.Time, bool) { return c.force.Deadline() }
func (c forceDetachedContext) Done() <-chan struct{}       { return c.force.Done() }
func (c forceDetachedContext) Err() error                  { return c.force.Err() }
func (c forceDetachedContext) Value(key any) any {
	if value := c.Context.Value(key); value != nil {
		return value
	}
	// Preserve context.Cause's internal cancellation lookup as well.
	return c.force.Value(key)
}

func detachExceptForce(ctx context.Context) context.Context {
	detached := context.WithoutCancel(ctx)
	if force, ok := ctx.Value(forceCancellationKey{}).(context.Context); ok {
		return forceDetachedContext{Context: detached, force: force}
	}
	return detached
}
