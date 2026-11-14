package httputil

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

type forceHTTPResponseKey struct{}

// The response controller lives below Gin, whose writer may hide deadline
// support. Expiring an HTTP/2 write deadline interrupts only that stream.
type forceHTTPResponse struct {
	mu         sync.Mutex
	controller *http.ResponseController
	forced     bool
	finished   bool
	hijacked   net.Conn
}

// WithoutHTTPResponseInterrupt detaches background jobs from the submission
// response. The job owns its upstream work, but never that old HTTP transport.
func WithoutHTTPResponseInterrupt(ctx context.Context) context.Context {
	return context.WithValue(ctx, forceHTTPResponseKey{}, (*forceHTTPResponse)(nil))
}

func InterruptHTTPResponse(ctx context.Context) {
	control, _ := ctx.Value(forceHTTPResponseKey{}).(*forceHTTPResponse)
	if control == nil {
		return
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.finished {
		return
	}
	control.forced = true
	if control.hijacked != nil {
		// coder/websocket CloseNow can wait behind an already-started close
		// handshake. The owned hijacked transport must be closed directly.
		_ = control.hijacked.Close()
		return
	}
	_ = control.controller.SetWriteDeadline(time.Now())
}
