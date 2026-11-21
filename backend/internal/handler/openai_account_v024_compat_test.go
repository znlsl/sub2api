//go:build unit

package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// The v0.2.24 duplicate-field guard must remain effective in every WS mode,
// and rejected first/subsequent frames must not leak an admission slot/session.
func TestAccountDisconnectV024DuplicateFrameCleanup(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool, service.OpenAIWSIngressModeDedicated, service.OpenAIWSIngressModeHTTPBridge} {
		for _, first := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/first=%t", mode, first), func(t *testing.T) {
				var calls atomic.Int32
				completed := `{"type":"response.completed","response":{"id":"resp_duplicate_guard","status":"completed","model":"gpt-5.1","output":[],"usage":{"input_tokens":2,"output_tokens":1}}}`
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if mode == service.OpenAIWSIngressModeHTTPBridge {
						_, _ = io.Copy(io.Discard, r.Body)
						calls.Add(1)
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: %s\n\n", completed)
						return
					}
					conn, err := coderws.Accept(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.CloseNow()
					for {
						if _, _, err := conn.Read(r.Context()); err != nil {
							return
						}
						calls.Add(1)
						if err := conn.Write(r.Context(), coderws.MessageText, []byte(completed)); err != nil {
							return
						}
					}
				}))
				defer upstream.Close()
				h, repo, cache, server := newDisconnectHandler(t, upstream.URL, mode)
				defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
				require.NoError(t, err)
				defer conn.CloseNow()
				if !first {
					require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","input":"hi","stream":true}`)))
					for {
						_, frame, err := conn.Read(ctx)
						require.NoError(t, err)
						if strings.Contains(string(frame), "response.completed") {
							break
						}
					}
				}
				payload := `{"type":"response.create","model":"gpt-5.1","input":"hi","stream":true,"reasoning":{"effort":"low","effort":"high"}}`
				if first {
					payload = `{"type":"response.create","model":"gpt-5.1","model":"gpt-5.1","input":"hi","stream":true}`
				}
				require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(payload)))
				_, _, err = conn.Read(ctx)
				require.Error(t, err)
				require.Equal(t, coderws.StatusPolicyViolation, coderws.CloseStatus(err))
				require.Contains(t, err.Error(), "more than once")
				wantCalls := int32(1)
				if first {
					wantCalls = 0
				}
				require.Equal(t, wantCalls, calls.Load(), "rejected frame must never reach upstream")
				require.True(t, repo.schedulingEnabled.Load(), "client validation must not disable the account")
				result, err := h.gatewayService.DisconnectAccount(ctx, 9901, 3*time.Second)
				require.NoError(t, err)
				require.True(t, result.Drained, "%+v", result)
				require.NotNil(t, result.CurrentConcurrency)
				require.Zero(t, *result.CurrentConcurrency)
				require.NoError(t, ctx.Err())
				cache.mu.Lock()
				acquisitions, releases := cache.acquisitions, cache.releases
				cache.mu.Unlock()
				require.Equal(t, acquisitions, releases)
			})
		}
	}
}

// v0.2.24 classifies upstream 408 as retryable. Graceful disable must still
// allow that failover, whereas an operator abort during the 408 body read must
// cancel transport without replaying the old request on the eligible backup.
func TestAccountDisconnectV024RequestTimeoutBoundary(t *testing.T) {
	for _, route := range []struct{ name, path, body string }{
		{"responses", "/v1/responses", `{"model":"gpt-5.1","input":"hi","stream":true}`},
		{"images", "/v1/images/generations", `{"model":"gpt-image-1","prompt":"a tree","n":1}`},
	} {
		for _, action := range []string{"normal", "disable", "force"} {
			t.Run(route.name+"/"+action, func(t *testing.T) {
				started, proceed, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var aCalls, bCalls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					if r.Header.Get("Authorization") == "Bearer sk-backup" {
						bCalls.Add(1)
						if route.name == "images" {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{"created":1,"data":[{"b64_json":"YmFja3VwLWltYWdl"}]}`)
						} else {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_408_backup\",\"status\":\"completed\",\"model\":\"gpt-5.1\",\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n")
						}
						return
					}
					if aCalls.Add(1) != 1 {
						t.Error("account A replayed after 408")
						return
					}
					defer close(stopped)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusRequestTimeout)
					if action == "force" {
						_, _ = io.WriteString(w, `{"error":`)
						w.(http.Flusher).Flush()
					}
					close(started)
					select {
					case <-proceed:
						_, _ = io.WriteString(w, `{"error":{"message":"Upstream request failed","type":"adobe_upstream"}}`)
					case <-r.Context().Done():
					}
				}))
				defer upstream.Close()
				h, repo, cache, server := newDisconnectHandler(t, upstream.URL, "", disconnectHandlerOptions{backup: true})
				defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				type result struct {
					status int
					body   string
					err    error
				}
				done := make(chan result, 1)
				go func() {
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+route.path, strings.NewReader(route.body))
					req.Header.Set("Content-Type", "application/json")
					res, err := server.Client().Do(req)
					if err != nil {
						done <- result{err: err}
						return
					}
					defer res.Body.Close()
					body, err := io.ReadAll(res.Body)
					done <- result{status: res.StatusCode, body: string(body), err: err}
				}()
				awaitDisconnectSignal(t, started, "upstream 408 attempt")
				if action == "force" {
					receipt, err := h.gatewayService.DisconnectAccount(ctx, 9901, 3*time.Second)
					require.NoError(t, err)
					require.True(t, receipt.Drained, "%+v", receipt)
					require.Equal(t, 1, receipt.CancellationRequested.HTTP)
				} else {
					if action == "disable" {
						require.NoError(t, repo.SetSchedulable(ctx, 9901, false))
					}
					close(proceed)
				}
				var got result
				select {
				case got = <-done:
				case <-ctx.Done():
					t.Fatal("request did not finish before deadline")
				}
				require.NoError(t, ctx.Err())
				awaitDisconnectSignal(t, stopped, "408 upstream closed")
				require.Equal(t, int32(1), aCalls.Load())
				if action == "force" {
					require.Error(t, got.err)
					require.Zero(t, bCalls.Load(), "force abort must not replay on backup")
					require.Zero(t, h.gatewayService.SnapshotOpenAIAccountSchedulerMetrics().RuntimeStatsAccountCount)
				} else {
					require.NoError(t, got.err)
					require.Equal(t, http.StatusOK, got.status, got.body)
					require.Equal(t, int32(1), bCalls.Load())
					if route.name == "images" {
						require.Contains(t, got.body, "YmFja3VwLWltYWdl")
					} else {
						require.Contains(t, got.body, "resp_408_backup")
					}
				}
				require.Equal(t, action == "normal", repo.schedulingEnabled.Load())
				require.Eventually(t, func() bool {
					cache.mu.Lock()
					defer cache.mu.Unlock()
					return cache.acquisitions > 0 && cache.acquisitions == cache.releases
				}, time.Second, time.Millisecond)
				cache.assertReleasedOnce(t)
			})
		}
	}
}
