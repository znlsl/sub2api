//go:build unit

package handler

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// v0.2.23 introduces idle failover before semantic output. Exercise the real
// handler with that timer enabled and an eligible backup, so an administrative
// abort cannot silently turn into a replay. The control case verifies that
// ordinary idle failover still succeeds through the same admission/registry.
func TestAccountDisconnectWithIdleFailoverEnabled(t *testing.T) {
	for _, tc := range []struct {
		name          string
		force, output bool
	}{
		{name: "force_before_output", force: true},
		{name: "force_after_output", force: true, output: true},
		{name: "ordinary_idle_selects_backup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var aCalls, bCalls atomic.Int32
			stopped := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				if r.Header.Get("Authorization") == "Bearer sk-backup" {
					bCalls.Add(1)
					_, _ = fmt.Fprint(w, `data: {"type":"response.completed","response":{"id":"resp_idle_backup","status":"completed","model":"gpt-5.1","output":[],"usage":{"input_tokens":2,"output_tokens":1}}}`+"\n\n")
					return
				}
				if aCalls.Add(1) != 1 {
					t.Error("account A was replayed")
					return
				}
				defer close(stopped)
				_, _ = fmt.Fprint(w, `data: {"type":"response.created","response":{"id":"resp_idle_a","model":"gpt-5.1"}}`+"\n\n")
				if tc.output {
					_, _ = fmt.Fprint(w, `data: {"type":"response.output_text.delta","delta":"visible-a"}`+"\n\n")
				}
				_, _ = fmt.Fprint(w, "event: keepalive\ndata: {\"type\":\"keepalive\"}\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer upstream.Close()
			h, repo, cache, server := newDisconnectHandler(t, upstream.URL, "", disconnectHandlerOptions{backup: true})
			h.cfg.Gateway.StreamDataIntervalTimeout = 2
			h.cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 30
			h.cfg.Gateway.StreamKeepaliveInterval = 1
			defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)

			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.1","input":"hi","stream":true}`))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			visible := make(chan struct{})
			type clientResult struct {
				body string
				err  error
			}
			done := make(chan clientResult, 1)
			go func() {
				res, err := server.Client().Do(req)
				if err != nil {
					done <- clientResult{err: err}
					return
				}
				defer res.Body.Close()
				reader := bufio.NewReader(res.Body)
				var body strings.Builder
				var first sync.Once
				for {
					line, readErr := reader.ReadString('\n')
					body.WriteString(line)
					if (tc.output && strings.Contains(line, "visible-a")) || (!tc.output && (strings.Contains(line, "keepalive") || strings.HasPrefix(line, ":"))) {
						first.Do(func() { close(visible) })
					}
					if readErr != nil {
						done <- clientResult{body: body.String(), err: readErr}
						return
					}
				}
			}()
			awaitDisconnectSignal(t, visible, "client output while idle timer is armed")
			if tc.force {
				result, err := h.gatewayService.DisconnectAccount(ctx, 9901, 3*time.Second)
				require.NoError(t, err)
				require.True(t, result.Drained, "%+v", result)
				require.Equal(t, 1, result.CancellationRequested.HTTP)
				require.False(t, repo.schedulingEnabled.Load())
			}
			var client clientResult
			select {
			case client = <-done:
			case <-ctx.Done():
				t.Fatal("client did not finish before test deadline")
			}
			require.NoError(t, ctx.Err())
			awaitDisconnectSignal(t, stopped, "account A upstream closed")
			require.Equal(t, int32(1), aCalls.Load())
			if tc.force {
				require.Error(t, client.err)
				require.NotErrorIs(t, client.err, io.EOF, "administrative abort must be a transport failure")
				require.NotContains(t, client.body, "response.completed")
				require.NotContains(t, client.body, "stream_timeout")
				require.Zero(t, bCalls.Load(), "administrative abort must not replay the old request")
				require.Zero(t, h.gatewayService.SnapshotOpenAIAccountSchedulerMetrics().RuntimeStatsAccountCount)
			} else {
				require.ErrorIs(t, client.err, io.EOF)
				require.Contains(t, client.body, "resp_idle_backup")
				require.NotContains(t, client.body, "stream_timeout")
				require.Equal(t, int32(1), bCalls.Load())
				require.True(t, repo.schedulingEnabled.Load(), "ordinary idle handling must not invoke admin disable")
			}
			require.Eventually(t, func() bool {
				cache.mu.Lock()
				defer cache.mu.Unlock()
				return cache.acquisitions > 0 && cache.acquisitions == cache.releases
			}, time.Second, 5*time.Millisecond)
			cache.assertReleasedOnce(t)
		})
	}
}
