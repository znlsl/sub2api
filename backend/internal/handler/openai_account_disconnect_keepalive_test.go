//go:build unit

package handler

import (
	"bufio"
	"context"
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

// v0.2.22 forwards keepalives before semantic output and lets Messages fail
// over after keepalive-only writes. An administrator abort must still stop the
// current transport without becoming a retry, a health failure or a success.
func TestForceDisconnectAfterPreOutputKeepalive(t *testing.T) {
	for _, tc := range []struct{ name, path, body string }{
		{"responses", "/v1/responses", `{"model":"gpt-5.1","input":"hi","stream":true}`},
		{"messages", "/v1/messages", `{"model":"gpt-5.1","messages":[{"role":"user","content":"hi"}],"max_tokens":20,"stream":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stopped := make(chan struct{})
			var upstreamCalls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if upstreamCalls.Add(1) != 1 {
					t.Error("administrator cancellation was retried upstream")
					http.Error(w, "unexpected retry", http.StatusBadGateway)
					return
				}
				defer close(stopped)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_keepalive\",\"model\":\"gpt-5.1\"}}\n\n")
				_, _ = io.WriteString(w, "event: keepalive\ndata: {\"type\":\"keepalive\"}\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer upstream.Close()
			h, repo, cache, server := newDisconnectHandler(t, upstream.URL, "")
			h.cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 30
			h.cfg.Gateway.StreamKeepaliveInterval = 1
			defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+tc.path, strings.NewReader(tc.body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			keepalive := make(chan struct{})
			type clientResult struct {
				status int
				body   string
				err    error
			}
			done := make(chan clientResult, 1)
			go func() {
				res, requestErr := server.Client().Do(req)
				if requestErr != nil {
					done <- clientResult{err: requestErr}
					return
				}
				defer res.Body.Close()
				var body strings.Builder
				var first sync.Once
				reader := bufio.NewReader(res.Body)
				for {
					line, readErr := reader.ReadString('\n')
					body.WriteString(line)
					if strings.Contains(line, "keepalive") || strings.Contains(line, "ping") {
						first.Do(func() { close(keepalive) })
					}
					if readErr != nil {
						done <- clientResult{status: res.StatusCode, body: body.String(), err: readErr}
						return
					}
				}
			}()
			awaitDisconnectSignal(t, keepalive, "pre-output keepalive at client")
			result, err := h.gatewayService.DisconnectAccount(context.Background(), 9901, 3*time.Second)
			require.NoError(t, err)
			require.True(t, result.Drained, "%+v", result)
			require.Equal(t, 1, result.CancellationRequested.HTTP)
			require.False(t, repo.schedulingEnabled.Load())
			awaitDisconnectSignal(t, stopped, "upstream cancelled after keepalive")
			select {
			case client := <-done:
				require.Equal(t, http.StatusOK, client.status, "keepalive already committed HTTP 200")
				require.Error(t, client.err)
				require.NotErrorIs(t, client.err, io.EOF, "force disconnect must remain a transport failure")
				require.NotContains(t, client.body, "response.completed")
				require.NotContains(t, client.body, "message_stop")
			case <-ctx.Done():
				t.Fatal("client did not observe force disconnect before its timeout")
			}
			require.NoError(t, ctx.Err())
			require.Equal(t, int32(1), upstreamCalls.Load())
			require.Zero(t, h.gatewayService.SnapshotOpenAIAccountSchedulerMetrics().RuntimeStatsAccountCount)
			cache.assertReleasedOnce(t)
		})
	}
}
