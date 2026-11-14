package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

type disconnectRepo struct {
	AccountRepository
	enabled  atomic.Bool
	setErr   error
	platform string
	onSet    func()
}

func (r *disconnectRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	if id != 1 {
		return nil, ErrAccountNotFound
	}
	return &Account{ID: id, Platform: r.platform, Status: StatusActive, Schedulable: r.enabled.Load()}, nil
}
func (r *disconnectRepo) SetSchedulable(ctx context.Context, id int64, enabled bool) error {
	if r.setErr != nil {
		return r.setErr
	}
	r.enabled.Store(enabled)
	if r.onSet != nil {
		r.onSet()
	}
	return nil
}
func (r *disconnectRepo) IsAccountSchedulingEnabled(ctx context.Context, id int64) (bool, error) {
	return r.enabled.Load(), nil
}

type disconnectConcurrency struct {
	ConcurrencyCache
	count   atomic.Int32
	err     error
	missing bool
}

func (c *disconnectConcurrency) GetAccountConcurrencyBatch(context.Context, []int64) (map[int64]int, error) {
	if c.missing {
		return map[int64]int{}, nil
	}
	return map[int64]int{1: int(c.count.Load())}, c.err
}
func disconnectService(t *testing.T) (*OpenAIGatewayService, *disconnectRepo, *disconnectConcurrency) {
	t.Helper()
	repo := &disconnectRepo{platform: PlatformOpenAI}
	repo.enabled.Store(true)
	cache := &disconnectConcurrency{}
	svc := &OpenAIGatewayService{accountRepo: repo, concurrencyService: NewConcurrencyService(cache), cfg: &config.Config{}}
	t.Cleanup(svc.CloseOpenAIWSPool)
	return svc, repo, cache
}

func TestAccountDisconnectCancelsOnlyTargetAndWaitsForActualCompletion(t *testing.T) {
	svc, repo, cache := disconnectService(t)
	active, finish, err := svc.BeginAccountRequest(context.Background(), 1, AccountRequestHTTP, nil)
	require.NoError(t, err)
	idle, finishIdle, err := svc.BeginAccountRequest(context.Background(), 1, AccountRequestWebSocket, nil)
	require.NoError(t, err)
	other, finishOther, err := svc.BeginAccountRequest(context.Background(), 2, AccountRequestHTTP, nil)
	require.NoError(t, err)
	defer finishOther()
	cache.count.Store(1)
	upstream, release := pkghttputil.NewUpstreamAttemptContext(pkghttputil.DetachUpstreamContext(active), time.Hour)
	defer release()
	result, err := svc.DisconnectAccount(context.Background(), 1, 0)
	require.NoError(t, err)
	require.False(t, repo.enabled.Load())
	require.ErrorIs(t, context.Cause(active), ErrAccountRequestsDisconnected)
	require.ErrorIs(t, context.Cause(idle), ErrAccountRequestsDisconnected)
	require.NoError(t, other.Err())
	select {
	case <-upstream.Done():
	case <-time.After(time.Second):
		t.Fatal("force cancellation lost in detached upstream")
	}
	require.False(t, result.Drained, "cancellation is not completion")
	require.Equal(t, AccountRequestCounts{HTTP: 1, WebSockets: 1}, result.Remaining)
	require.Equal(t, 1, *result.CurrentConcurrency)
	cache.count.Store(0)
	result, err = svc.DisconnectAccount(context.Background(), 1, 0)
	require.NoError(t, err)
	require.False(t, result.Drained, "early Redis release must not hide active forwarding")
	finish()
	finish()
	finishIdle()
	result, err = svc.DisconnectAccount(context.Background(), 1, time.Second)
	require.NoError(t, err)
	require.True(t, result.Drained)
	require.Equal(t, AccountRequestCounts{}, result.Remaining)
	require.NoError(t, other.Err())
	// No permanently retained tombstone: explicit later re-enable can admit new work.
	repo.enabled.Store(true)
	next, finishNext, err := svc.BeginAccountRequest(context.Background(), 1, AccountRequestHTTP, nil)
	require.NoError(t, err)
	defer finishNext()
	require.NoError(t, next.Err())
}

func TestAccountDisconnectWaitsForCleanup(t *testing.T) {
	svc, _, cache := disconnectService(t)
	ctx, finish, err := svc.BeginAccountRequest(context.Background(), 1, AccountRequestHTTP, nil)
	require.NoError(t, err)
	cache.count.Store(1)
	go func() { <-ctx.Done(); cache.count.Store(0); finish() }()
	result, err := svc.DisconnectAccount(context.Background(), 1, time.Second)
	require.NoError(t, err)
	require.True(t, result.Drained)
	require.False(t, result.TimedOut)
}

func TestAccountDisconnectRefusesNewAdmissionDuringDisable(t *testing.T) {
	svc, repo, _ := disconnectService(t)
	repo.onSet = func() {
		_, _, err := svc.BeginAccountRequest(context.Background(), 1, AccountRequestHTTP, nil)
		require.ErrorIs(t, err, ErrAccountSchedulingDisabled)
	}
	result, err := svc.DisconnectAccount(context.Background(), 1, 0)
	require.NoError(t, err)
	require.True(t, result.Drained)
	require.ErrorIs(t, svc.CheckAccountScheduling(context.Background(), 1), ErrAccountSchedulingDisabled)
}

func TestAccountDisconnectFailureDoesNotClaimSuccess(t *testing.T) {
	for _, kind := range []string{"db", "redis", "missing_count", "residual_count", "unsupported", "missing_account", "reenabled"} {
		t.Run(kind, func(t *testing.T) {
			svc, repo, cache := disconnectService(t)
			id := int64(1)
			ctx, finish, err := svc.BeginAccountRequest(context.Background(), 1, AccountRequestHTTP, nil)
			require.NoError(t, err)
			if kind != "db" && kind != "unsupported" && kind != "missing_account" {
				finish()
			} else {
				defer finish()
			}
			switch kind {
			case "db":
				repo.setErr = errors.New("database unavailable")
			case "redis":
				cache.err = errors.New("redis unavailable")
			case "missing_count":
				cache.missing = true
			case "residual_count":
				cache.count.Store(1)
			case "unsupported":
				repo.platform = PlatformGemini
			case "missing_account":
				id = 99
			case "reenabled":
				repo.onSet = func() { repo.enabled.Store(true) }
			}
			result, err := svc.DisconnectAccount(context.Background(), id, 0)
			if kind == "residual_count" {
				require.NoError(t, err)
				require.False(t, result.Drained)
				return
			}
			require.Error(t, err)
			if result != nil {
				require.False(t, result.Drained)
			}
			if kind == "db" || kind == "unsupported" || kind == "missing_account" {
				require.NoError(t, ctx.Err())
				require.True(t, repo.enabled.Load())
			}
		})
	}
}

func TestAccountRequestRegistryConcurrentAbortAndFinish(t *testing.T) {
	var registry accountRequestRegistry
	var callbacks atomic.Int32
	for i := 0; i < 100; i++ {
		parent, cancel := context.WithCancel(context.Background())
		ctx, finish, err := registry.begin(parent, 1, AccountRequestHTTP, func() { callbacks.Add(1) })
		require.NoError(t, err)
		var group sync.WaitGroup
		group.Add(3)
		go func() { defer group.Done(); registry.cancelAccount(1) }()
		go func() { defer group.Done(); finish() }()
		go func() { defer group.Done(); cancel() }()
		group.Wait()
		require.True(t, registry.counts(1).empty())
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	}
	require.LessOrEqual(t, callbacks.Load(), int32(100))
}

type forceCloseProbe struct {
	openAIWSFakeConn
	aborted  atomic.Int32
	graceful atomic.Int32
}

func (c *forceCloseProbe) CloseNow() error { c.aborted.Add(1); return nil }
func (c *forceCloseProbe) Close() error    { c.graceful.Add(1); return nil }
func TestOpenAIWSConnPoolAbortAccountIsImmediateAndIsolated(t *testing.T) {
	pool := newOpenAIWSConnPool(&config.Config{})
	defer pool.Close()
	a, b := &forceCloseProbe{}, &forceCloseProbe{}
	ca, cb := newOpenAIWSConn("a", 1, a, nil), newOpenAIWSConn("b", 2, b, nil)
	pool.getOrCreateAccountPool(1).conns[ca.id] = ca
	pool.getOrCreateAccountPool(2).conns[cb.id] = cb
	require.Equal(t, 1, pool.AbortAccount(1))
	require.Equal(t, int32(1), a.aborted.Load())
	require.Zero(t, a.graceful.Load())
	require.Zero(t, b.aborted.Load())
	require.Zero(t, b.graceful.Load())
	require.Equal(t, 0, pool.AbortAccount(1))
}

func TestForceDisconnectPoolOvertakesGracefulUpstreamClose(t *testing.T) {
	accepted := make(chan struct{})
	releasePeer := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		close(accepted)
		// A peer that does not read cannot reply to a graceful close frame.
		<-releasePeer
	}))
	defer server.Close()
	defer close(releasePeer)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	raw, _, _, err := newDefaultOpenAIWSClientDialer().Dial(ctx, server.URL, nil, "")
	require.NoError(t, err)
	conn := newOpenAIWSConn("force-race", 1, raw, nil)
	<-accepted
	closing := make(chan struct{})
	go func() { conn.close(); close(closing) }()
	select {
	case <-conn.closedCh:
	case <-time.After(time.Second):
		t.Fatal("graceful close not started")
	}
	forced := make(chan struct{})
	go func() { conn.abort(); close(forced) }()
	select {
	case <-forced:
	case <-time.After(time.Second):
		t.Fatal("force close waited for graceful peer acknowledgment")
	}
	select {
	case <-closing:
	case <-time.After(time.Second):
		t.Fatal("graceful closer remained blocked")
	}
}

func TestAccountDisconnectCompletesSignalsAfterManagementClientLeaves(t *testing.T) {
	svc, repo, _ := disconnectService(t)
	active, finish, err := svc.BeginAccountRequest(context.Background(), 1, AccountRequestHTTP, nil)
	require.NoError(t, err)
	defer finish()
	management, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo.onSet = cancel
	result, err := svc.DisconnectAccount(management, 1, 0)
	require.NoError(t, err)
	require.ErrorIs(t, pkghttputil.ForceCancellationCause(active), ErrAccountRequestsDisconnected)
	require.False(t, repo.enabled.Load())
	require.False(t, result.Drained)
}

func TestAccountRequestRegistryConcurrentDisconnectsKeepGateClosed(t *testing.T) {
	var r accountRequestRegistry
	endFirst := r.startDrain(1)
	endSecond := r.startDrain(1)
	endFirst()
	_, _, err := r.begin(context.Background(), 1, AccountRequestHTTP, nil)
	require.ErrorIs(t, err, ErrAccountSchedulingDisabled)
	endSecond()
	_, finish, err := r.begin(context.Background(), 1, AccountRequestHTTP, nil)
	require.NoError(t, err)
	finish()
	require.Empty(t, r.active)
	require.Empty(t, r.draining)
}

func TestAccountRequestRegistryFinishAcknowledgesTransportInterrupt(t *testing.T) {
	var registry accountRequestRegistry
	for i := 0; i < 200; i++ {
		var interrupted atomic.Bool
		_, finish, err := registry.begin(context.Background(), 1, AccountRequestHTTP, func() { interrupted.Store(true) })
		require.NoError(t, err)
		registry.cancelAccount(1)
		finish()
		require.True(t, interrupted.Load(), "completion must follow the transport interruption, even when forwarding exits immediately")
		require.True(t, registry.counts(1).empty())
	}
}

func TestAccountDisconnectCancelsPendingPrewarmHandshake(t *testing.T) {
	svc, _, _ := disconnectService(t)
	started, stopped, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) != 1 {
			t.Error("disabled account started another prewarm")
			return
		}
		close(started)
		<-r.Context().Done()
		close(stopped)
	}))
	defer upstream.Close()
	pool := svc.getOpenAIWSConnPool()
	defer pool.AbortAccount(1)
	req := openAIWSAcquireRequest{Account: &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}, WSURL: upstream.URL}
	ap := pool.getOrCreateAccountPool(1)
	ap.mu.Lock()
	ap.lastAcquire = &req
	ap.creating = 3
	ap.prewarmActive = true
	ap.mu.Unlock()
	go func() { pool.prewarmConns(1, req, 3, 0); close(finished) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("prewarm handshake never started")
	}
	result, err := svc.DisconnectAccount(context.Background(), 1, 2*time.Second)
	require.NoError(t, err)
	require.True(t, result.Drained, "%+v", result)
	require.Zero(t, result.RemainingUpstreamConnections)
	for _, ch := range []chan struct{}{stopped, finished} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("pending handshake survived account disconnect")
		}
	}
	require.Equal(t, int32(1), calls.Load())
	require.Zero(t, pool.AccountConnectionCount(1))
}

func TestAccountDisconnectDoesNotHidePendingPoolCleanup(t *testing.T) {
	svc, _, _ := disconnectService(t)
	pool := svc.getOpenAIWSConnPool()
	ap := pool.getOrCreateAccountPool(1)
	ap.creating = 1
	result, err := svc.DisconnectAccount(context.Background(), 1, 0)
	require.NoError(t, err)
	require.False(t, result.Drained)
	require.True(t, result.TimedOut)
	require.Equal(t, 1, result.RemainingUpstreamConnections)
	ap.creating = 0
	result, err = svc.DisconnectAccount(context.Background(), 1, 0)
	require.NoError(t, err)
	require.True(t, result.Drained)
}
