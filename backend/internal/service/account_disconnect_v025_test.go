//go:build unit

package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// v0.2.25 partitions reusable WS connections by reported user identity.
// An account drain must cover every such identity, including idle connections,
// without closing another account or preventing an explicit later re-enable.
func TestAccountDisconnectV025ReportedIdentityConnections(t *testing.T) {
	closed := make(chan string, 8)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		identity := r.Header.Get("X-End-User")
		defer func() { conn.CloseNow(); closed <- identity }()
		for {
			_, payload, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if err := conn.Write(r.Context(), coderws.MessageText, append([]byte(identity+":"), payload...)); err != nil {
				return
			}
		}
	}))
	defer upstream.Close()
	svc, repo, _ := disconnectService(t)
	svc.cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 2
	svc.cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	svc.cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 2
	pool := svc.getOpenAIWSConnPool()
	defer pool.AbortAccount(1)
	defer pool.AbortAccount(2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	acquire := func(accountID, userID int64) *openAIWSConnLease {
		t.Helper()
		account := headerOverrideVarsAccount(PlatformOpenAI, AccountTypeAPIKey, map[string]any{"x-end-user": "{{user_id}}"})
		account.ID = accountID
		scoped := WithHeaderOverrideRequestScope(ctx, userID, 31, nil)
		headers, _, err := svc.buildOpenAIWSHeaders(scoped, nil, account, "fixture-key", OpenAIWSProtocolDecision{}, false, "", "", "", "", "")
		require.NoError(t, err)
		lease, err := pool.Acquire(scoped, openAIWSAcquireRequest{Account: account, WSURL: "ws" + strings.TrimPrefix(upstream.URL, "http"), Headers: headers})
		require.NoError(t, err)
		t.Cleanup(lease.Release)
		return lease
	}
	echo := func(lease *openAIWSConnLease, userID int64) {
		t.Helper()
		require.NoError(t, lease.WriteJSONWithContextTimeout(ctx, map[string]string{"message": "probe"}, time.Second))
		data, err := lease.ReadMessageWithContextTimeout(ctx, time.Second)
		require.NoError(t, err)
		identity, payload, ok := strings.Cut(string(data), ":")
		require.True(t, ok)
		require.Equal(t, fmt.Sprint(userID), identity)
		require.JSONEq(t, `{"message":"probe"}`, payload)
	}
	active := acquire(1, 7)
	echo(active, 7)
	idle := acquire(1, 8)
	echo(idle, 8)
	require.NotEqual(t, active.ConnID(), idle.ConnID())
	idle.Release()
	other := acquire(2, 9)
	echo(other, 9)
	require.Equal(t, 2, pool.AccountConnectionCount(1))

	// Graceful disable still leaves the admitted connection usable, but
	// refuses the next turn at the database gate, including an identity scope.
	require.NoError(t, repo.SetSchedulable(ctx, 1, false))
	err := svc.CheckAccountScheduling(WithHeaderOverrideRequestScope(ctx, 7, 31, nil), 1)
	require.ErrorIs(t, err, ErrAccountSchedulingDisabled)
	echo(active, 7)

	receipt, err := svc.DisconnectAccount(ctx, 1, time.Second)
	require.NoError(t, err)
	require.True(t, receipt.Drained, "%+v", receipt)
	require.Equal(t, 2, receipt.ClosedUpstreamConnections)
	require.Zero(t, receipt.RemainingUpstreamConnections)
	observed := map[string]bool{}
	for range 2 {
		select {
		case identity := <-closed:
			observed[identity] = true
		case <-ctx.Done():
			t.Fatal("target upstream transport did not close")
		}
	}
	require.Equal(t, map[string]bool{"7": true, "8": true}, observed)
	require.Error(t, active.WriteJSONContext(ctx, map[string]string{"message": "rejected"}))
	require.Equal(t, 1, pool.AccountConnectionCount(2))
	echo(other, 9)

	require.NoError(t, repo.SetSchedulable(ctx, 1, true))
	require.NoError(t, svc.CheckAccountScheduling(ctx, 1))
	reopened := acquire(1, 7)
	require.NotEqual(t, active.ConnID(), reopened.ConnID())
	echo(reopened, 7)
	require.NoError(t, ctx.Err())
}

// A completed prewarm transfers its socket into the pool. Ending the prewarm
// task must not cancel that healthy socket; a later account abort still must.
func TestAccountDisconnectV025CompletedPrewarmRemainsUsable(t *testing.T) {
	stopped := make(chan struct{}, 4)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		defer func() { stopped <- struct{}{} }()
		for {
			kind, payload, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if err := conn.Write(r.Context(), kind, payload); err != nil {
				return
			}
		}
	}))
	defer upstream.Close()
	svc, _, _ := disconnectService(t)
	svc.cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	svc.cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	pool := svc.getOpenAIWSConnPool()
	defer pool.AbortAccount(1)
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	req := openAIWSAcquireRequest{Account: account, WSURL: "ws" + strings.TrimPrefix(upstream.URL, "http")}
	ap := pool.getOrCreateAccountPool(1)
	ap.mu.Lock()
	ap.lastAcquire = &req
	ap.creating = 1
	ap.prewarmActive = true
	generation := ap.generation
	ap.mu.Unlock()
	pool.prewarmConns(1, req, 1, generation)
	require.Equal(t, 1, pool.AccountConnectionCount(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, err := pool.Acquire(ctx, req)
	require.NoError(t, err)
	defer lease.Release()
	require.True(t, lease.Reused(), "completed prewarm must leave its established connection reusable")
	require.NoError(t, lease.WriteJSONContext(ctx, map[string]string{"message": "after-prewarm"}))
	payload, err := lease.ReadMessageContext(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `{"message":"after-prewarm"}`, string(payload))
	result, err := svc.DisconnectAccount(ctx, 1, time.Second)
	require.NoError(t, err)
	require.True(t, result.Drained, "%+v", result)
	require.Equal(t, 1, result.ClosedUpstreamConnections)
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("prewarmed upstream was not force-disconnected")
	}
}
