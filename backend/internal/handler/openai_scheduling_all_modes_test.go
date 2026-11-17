//go:build unit

package handler

import (
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

	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Exercise both the unchanged multi-turn path and graceful admission rejection
// with actual client/upstream transports in every supported WS ingress mode.
func TestAccountSchedulingAdmissionWebSocketAllModes(t *testing.T) {
	for _, mode := range []string{
		service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool,
		service.OpenAIWSIngressModeDedicated, service.OpenAIWSIngressModeHTTPBridge,
	} {
		for _, disable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disable=%t", mode, disable), func(t *testing.T) {
				started := make(chan struct{})
				continueFirst := make(chan struct{})
				var releaseFirst sync.Once
				release := func() { releaseFirst.Do(func() { close(continueFirst) }) }
				var upstreamTurns atomic.Int32
				response := func(n int32) string {
					return fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_admission_%d","model":"gpt-5.1","usage":{"input_tokens":2,"output_tokens":1}}}`, n)
				}
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					nextTurn := func() (int32, bool) {
						n := upstreamTurns.Add(1)
						if n == 1 {
							close(started)
							select {
							case <-continueFirst:
							case <-r.Context().Done():
								return n, false
							}
						}
						return n, true
					}
					if mode == service.OpenAIWSIngressModeHTTPBridge {
						_, _ = io.Copy(io.Discard, r.Body)
						n, ok := nextTurn()
						if !ok {
							return
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", response(n))
						w.(http.Flusher).Flush()
						return
					}
					conn, err := coderws.Accept(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.CloseNow()
					ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
					defer cancel()
					for {
						if _, _, err := conn.Read(ctx); err != nil {
							return
						}
						n, ok := nextTurn()
						if !ok {
							return
						}
						if err := conn.Write(ctx, coderws.MessageText, []byte(response(n))); err != nil {
							return
						}
					}
				}))
				defer upstream.Close()
				defer release()
				h, repo, cache, server := newDisconnectHandler(t, upstream.URL, mode)
				defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses", nil)
				require.NoError(t, err)
				defer conn.CloseNow()
				writeTurn := func() {
					require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","input":"hi","stream":true}`)))
				}
				readCompleted := func() {
					for {
						_, payload, err := conn.Read(ctx)
						require.NoError(t, err)
						kind := gjson.GetBytes(payload, "type").String()
						require.NotEqual(t, "error", kind, string(payload))
						require.NotEqual(t, "response.failed", kind, string(payload))
						if kind == "response.completed" {
							return
						}
					}
				}
				writeTurn()
				awaitDisconnectSignal(t, started, "first upstream WS turn")
				if disable {
					require.NoError(t, repo.SetSchedulable(ctx, 9901, false))
					counts, err := cache.GetAccountConcurrencyBatch(ctx, []int64{9901})
					require.NoError(t, err)
					require.Equal(t, 1, counts[9901], "ordinary disable must retain the active turn's slot")
				}
				release()
				readCompleted()
				require.Eventually(t, func() bool {
					counts, _ := cache.GetAccountConcurrencyBatch(ctx, []int64{9901})
					return counts[9901] == 0
				}, time.Second, time.Millisecond, "completed first turn must release its slot")
				writeTurn()
				if disable {
					_, _, err := conn.Read(ctx)
					require.Error(t, err)
					require.Equal(t, coderws.StatusTryAgainLater, coderws.CloseStatus(err))
					require.Contains(t, err.Error(), "account is no longer schedulable")
					require.Equal(t, int32(1), upstreamTurns.Load(), "disabled second turn must not reach upstream")
				} else {
					readCompleted()
					require.Equal(t, int32(2), upstreamTurns.Load(), "enabled session must support another complete turn")
				}
				require.NoError(t, ctx.Err(), "result must arrive before the client timeout")
				require.Eventually(t, func() bool {
					counts, _ := cache.GetAccountConcurrencyBatch(ctx, []int64{9901})
					return counts[9901] == 0
				}, time.Second, time.Millisecond)
				cache.assertReleasedOnce(t)
			})
		}
	}
}

// A normal scheduling toggle must never become the force-disconnect operation:
// the admitted HTTP response still finishes and a fresh request is rechecked.
func TestAccountSchedulingAdmissionHTTPGracefulAllModes(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, disable := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/disable=%t", stream, disable), func(t *testing.T) {
				started := make(chan struct{})
				continueFirst := make(chan struct{})
				var releaseFirst sync.Once
				release := func() { releaseFirst.Do(func() { close(continueFirst) }) }
				var calls atomic.Int32
				completed := `{"id":"resp_graceful","status":"completed","model":"gpt-5.1","output":[],"usage":{"input_tokens":2,"output_tokens":1}}`
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					n := calls.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_graceful\",\"output_index\":0,\"content_index\":0,\"delta\":\"partial\"}\n\n")
						w.(http.Flusher).Flush()
					}
					if n == 1 {
						close(started)
						select {
						case <-continueFirst:
						case <-r.Context().Done():
							return
						}
					}
					if stream {
						_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":%s}\n\ndata: [DONE]\n\n", completed)
					} else {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, completed)
					}
				}))
				defer upstream.Close()
				defer release()
				h, repo, cache, server := newDisconnectHandler(t, upstream.URL, "")
				defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				type clientResult struct {
					status int
					body   string
					err    error
				}
				request := func() clientResult {
					payload := fmt.Sprintf(`{"model":"gpt-5.1","input":"hi","stream":%t}`, stream)
					req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader(payload))
					if err != nil {
						return clientResult{err: err}
					}
					req.Header.Set("Content-Type", "application/json")
					res, err := server.Client().Do(req)
					if err != nil {
						return clientResult{err: err}
					}
					defer res.Body.Close()
					body, err := io.ReadAll(res.Body)
					return clientResult{status: res.StatusCode, body: string(body), err: err}
				}
				done := make(chan clientResult, 1)
				go func() { done <- request() }()
				awaitDisconnectSignal(t, started, "first HTTP upstream request")
				if disable {
					require.NoError(t, repo.SetSchedulable(ctx, 9901, false))
					counts, err := cache.GetAccountConcurrencyBatch(ctx, []int64{9901})
					require.NoError(t, err)
					require.Equal(t, 1, counts[9901], "normal disable must retain the admitted HTTP slot")
				}
				release()
				select {
				case first := <-done:
					require.NoError(t, first.err)
					require.Equal(t, http.StatusOK, first.status)
					require.Contains(t, first.body, "resp_graceful")
					if stream {
						require.Contains(t, first.body, "partial")
						require.Contains(t, first.body, "response.completed")
					}
				case <-ctx.Done():
					t.Fatal("admitted HTTP response did not complete")
				}
				second := request()
				require.NoError(t, second.err)
				if disable {
					require.GreaterOrEqual(t, second.status, http.StatusBadRequest)
					require.Equal(t, int32(1), calls.Load(), "disabled account must not receive the next request")
				} else {
					require.Equal(t, http.StatusOK, second.status)
					require.Contains(t, second.body, "resp_graceful")
					require.Equal(t, int32(2), calls.Load())
				}
				require.NoError(t, ctx.Err())
				cache.assertReleasedOnce(t)
			})
		}
	}
}
