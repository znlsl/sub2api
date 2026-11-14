package service

import (
	"context"
	"fmt"
	"sync"

	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
)

// ErrAccountRequestsDisconnected is an operator cancellation, not an upstream failure.
var ErrAccountRequestsDisconnected = fmt.Errorf("account requests disconnected by administrator: %w", context.Canceled)

type AccountRequestKind string

const (
	AccountRequestHTTP      AccountRequestKind = "http"
	AccountRequestWebSocket AccountRequestKind = "websocket"
)

type accountRequestEntry struct {
	mu          sync.Mutex
	kind        AccountRequestKind
	cancel      context.CancelCauseFunc
	forceCancel context.CancelCauseFunc
	interrupt   func()
	finished    bool
	cancelled   bool
	interrupted bool
}

func (e *accountRequestEntry) abort() {
	e.mu.Lock()
	if e.finished || e.cancelled {
		e.mu.Unlock()
		return
	}
	e.cancelled = true
	e.forceCancel(ErrAccountRequestsDisconnected)
	e.cancel(ErrAccountRequestsDisconnected)
	e.mu.Unlock()
	// Notify every request immediately; a slow socket must not delay the others.
	go func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.interruptLocked()
	}()
}

// interruptLocked also runs from finish: forwarding may unwind before the
// asynchronous abort callback starts, but completion must acknowledge the
// transport interrupt. Holding mu prevents a late deadline on a reused writer.
func (e *accountRequestEntry) interruptLocked() {
	if e.cancelled && !e.interrupted && e.interrupt != nil {
		e.interrupted = true
		e.interrupt()
	}
}

type accountRequestRegistry struct {
	mu       sync.Mutex
	active   map[int64]map[*accountRequestEntry]struct{}
	draining map[int64]int
}

type AccountRequestCounts struct {
	HTTP       int `json:"http"`
	WebSockets int `json:"websockets"`
}

func (c AccountRequestCounts) empty() bool { return c.HTTP == 0 && c.WebSockets == 0 }

// begin registers BEFORE the authoritative scheduling check. Consequently a
// racing disable either cancels this entry or is observed by that check.
func (r *accountRequestRegistry) begin(parent context.Context, accountID int64, kind AccountRequestKind, interrupt func()) (context.Context, func(), error) {
	if parent == nil {
		parent = context.Background()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.draining[accountID] > 0 {
		return nil, nil, ErrAccountSchedulingDisabled
	}
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	force, forceCancel := context.WithCancelCause(context.Background())
	ctx, cancel := context.WithCancelCause(pkghttputil.WithForceCancellation(parent, force))
	entry := &accountRequestEntry{kind: kind, cancel: cancel, forceCancel: forceCancel, interrupt: interrupt}
	if r.active == nil {
		r.active = make(map[int64]map[*accountRequestEntry]struct{})
	}
	if r.active[accountID] == nil {
		r.active[accountID] = make(map[*accountRequestEntry]struct{})
	}
	r.active[accountID][entry] = struct{}{}
	var once sync.Once
	finish := func() {
		once.Do(func() {
			entry.mu.Lock()
			entry.interruptLocked()
			entry.finished = true
			// Keep the caller context usable for billing and account failover.
			// Its incoming request still owns normal cancellation.
			entry.mu.Unlock()
			r.mu.Lock()
			delete(r.active[accountID], entry)
			if len(r.active[accountID]) == 0 {
				delete(r.active, accountID)
			}
			r.mu.Unlock()
		})
	}
	return ctx, finish, nil
}

func (r *accountRequestRegistry) startDrain(accountID int64) func() {
	r.mu.Lock()
	if r.draining == nil {
		r.draining = make(map[int64]int)
	}
	r.draining[accountID]++
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.draining[accountID]--
			if r.draining[accountID] == 0 {
				delete(r.draining, accountID)
			}
		})
	}
}

func (r *accountRequestRegistry) cancelAccount(accountID int64) AccountRequestCounts {
	r.mu.Lock()
	entries := make([]*accountRequestEntry, 0, len(r.active[accountID]))
	for entry := range r.active[accountID] {
		entries = append(entries, entry)
	}
	r.mu.Unlock()
	var counts AccountRequestCounts
	for _, entry := range entries {
		if entry.kind == AccountRequestWebSocket {
			counts.WebSockets++
		} else {
			counts.HTTP++
		}
		entry.abort()
	}
	return counts
}

func (r *accountRequestRegistry) counts(accountID int64) AccountRequestCounts {
	r.mu.Lock()
	defer r.mu.Unlock()
	var counts AccountRequestCounts
	for entry := range r.active[accountID] {
		if entry.kind == AccountRequestWebSocket {
			counts.WebSockets++
		} else {
			counts.HTTP++
		}
	}
	return counts
}

// BeginAccountRequest is used by gateway handlers, not by administrative probes.
// finish acknowledges actual forwarding completion, not merely cancellation.
func (s *OpenAIGatewayService) BeginAccountRequest(ctx context.Context, accountID int64, kind AccountRequestKind, interrupt func()) (context.Context, func(), error) {
	return s.accountRequests.begin(ctx, accountID, kind, interrupt)
}
