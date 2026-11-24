package handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Keep official-host capability checks while all test traffic stays local.
type disconnectHTTPUpstream struct {
	service.HTTPUpstream
	target *url.URL
}

func (u *disconnectHTTPUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	local := req.Clone(req.Context())
	local.URL.Scheme, local.URL.Host = u.target.Scheme, u.target.Host
	local.Host = u.target.Host
	return http.DefaultClient.Do(local)
}

type disconnectHandlerRepo struct {
	*openAIWSUsageHandlerAccountRepoStub
	other *service.Account
}

func (r *disconnectHandlerRepo) SetSchedulable(_ context.Context, id int64, enabled bool) error {
	if id != r.account.ID {
		return service.ErrAccountNotFound
	}
	r.schedulingEnabled.Store(enabled)
	return nil
}
func (r *disconnectHandlerRepo) GetByID(_ context.Context, id int64) (*service.Account, error) {
	if r.other != nil && id == r.other.ID {
		a := *r.other
		return &a, nil
	}
	if id != r.account.ID {
		return nil, service.ErrAccountNotFound
	}
	a := r.account
	a.Schedulable = r.schedulingEnabled.Load()
	return &a, nil
}

func (r *disconnectHandlerRepo) IsAccountSchedulingEnabled(ctx context.Context, id int64) (bool, error) {
	if r.other != nil && id == r.other.ID {
		return r.other.Schedulable, nil
	}
	return r.openAIWSUsageHandlerAccountRepoStub.IsAccountSchedulingEnabled(ctx, id)
}
func (r *disconnectHandlerRepo) ListSchedulableByPlatform(ctx context.Context, platform string) ([]service.Account, error) {
	accounts, err := r.openAIWSUsageHandlerAccountRepoStub.ListSchedulableByPlatform(ctx, platform)
	if r.other != nil && r.other.Platform == platform {
		accounts = append(accounts, *r.other)
	}
	return accounts, err
}
func (r *disconnectHandlerRepo) ListSchedulableByGroupIDAndPlatform(ctx context.Context, _ int64, platform string) ([]service.Account, error) {
	return r.ListSchedulableByPlatform(ctx, platform)
}

type disconnectHandlerOptions struct {
	cache  service.ConcurrencyCache
	sticky service.GatewayCache
	backup bool
}

type disconnectHandlerCache struct {
	*concurrencyCacheMock
	mu                     sync.Mutex
	slots                  map[int64]map[string]struct{}
	acquisitions, releases int
}

func (c *disconnectHandlerCache) AcquireAccountSlot(ctx context.Context, id int64, max int, request string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if len(c.slots[id]) >= max {
		return false, nil
	}
	if c.slots[id] == nil {
		c.slots[id] = map[string]struct{}{}
	}
	c.slots[id][request] = struct{}{}
	c.acquisitions++
	return true, nil
}
func (c *disconnectHandlerCache) ReleaseAccountSlot(_ context.Context, id int64, request string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.slots[id], request)
	c.releases++
	return nil
}
func (c *disconnectHandlerCache) GetAccountConcurrencyBatch(_ context.Context, ids []int64) (map[int64]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[int64]int{}
	for _, id := range ids {
		out[id] = len(c.slots[id])
	}
	return out, nil
}
func (c *disconnectHandlerCache) assertReleasedOnce(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.Greater(t, c.acquisitions, 0)
	require.Equal(t, c.acquisitions, c.releases)
}

func newDisconnectHandler(t *testing.T, upstreamURL, mode string, options ...disconnectHandlerOptions) (*OpenAIGatewayHandler, *disconnectHandlerRepo, *disconnectHandlerCache, *httptest.Server) {
	t.Helper()
	enabled := &atomic.Bool{}
	enabled.Store(true)
	account := service.Account{ID: 9901, Name: "disconnect-e2e", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": upstreamURL},
		Extra:       map[string]any{"openai_apikey_responses_websockets_v2_enabled": mode != "", "openai_apikey_responses_websockets_v2_mode": mode}}
	repo := &disconnectHandlerRepo{openAIWSUsageHandlerAccountRepoStub: &openAIWSUsageHandlerAccountRepoStub{account: account, schedulingEnabled: enabled}}
	var opts disconnectHandlerOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.backup {
		b := account
		b.ID = 9902
		b.Name = "backup"
		b.Priority = 1
		b.Credentials = map[string]any{"api_key": "sk-backup", "base_url": upstreamURL}
		repo.other = &b
	}
	cache := &disconnectHandlerCache{concurrencyCacheMock: &concurrencyCacheMock{
		acquireUserSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
	}, slots: map[int64]map[string]struct{}{}}
	var concurrencyCache service.ConcurrencyCache = cache
	if opts.cache != nil {
		concurrencyCache = opts.cache
	}
	concurrency := service.NewConcurrencyService(concurrencyCache)
	cfg := &config.Config{}
	cfg.RunMode = config.RunModeSimple
	cfg.Default.RateMultiplier = 1
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 10
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 10
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 10
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	usage := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 10)}
	target, err := url.Parse(upstreamURL)
	require.NoError(t, err)
	gateway := service.NewOpenAIGatewayService(repo, usage, nil, nil, nil, nil, opts.sticky, cfg, nil, concurrency,
		service.NewBillingService(cfg, nil), nil, billing, &disconnectHTTPUpstream{target: target}, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
	t.Cleanup(gateway.CloseOpenAIWSPool)
	h := &OpenAIGatewayHandler{maxAccountSwitches: 2, cfg: cfg, gatewayService: gateway, billingCacheService: billing, apiKeyService: &service.APIKeyService{},
		concurrencyHelper: NewConcurrencyHelper(concurrency, SSEPingFormatNone, time.Second)}
	groupID := int64(4201)
	key := &service.APIKey{ID: 1801, GroupID: &groupID, User: &service.User{ID: 1701, Status: service.StatusActive}}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), key)
		c.Request = c.Request.WithContext(service.WithHeaderOverrideRequestScope(c.Request.Context(), key.User.ID, key.ID, key.GroupID))
		userConcurrency := 1
		if opts.backup {
			userConcurrency = 3
		}
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1701, Concurrency: userConcurrency})
		c.Next()
	})
	router.GET("/openai/v1/responses", h.ResponsesWebSocket)
	for _, path := range []string{"/v1/responses", "/responses", "/backend-api/codex/responses", "/v1/responses/compact", "/responses/compact", "/backend-api/codex/responses/compact"} {
		router.POST(path, h.Responses)
	}
	for _, path := range []string{"/v1/responses", "/responses", "/backend-api/codex/responses"} {
		router.GET(path, h.ResponsesWebSocket)
	}
	router.POST("/v1/chat/completions", h.ChatCompletions)
	router.POST("/v1/messages", h.Messages)
	router.POST("/v1/embeddings", h.Embeddings)
	router.POST("/v1/images/generations", h.Images)
	router.POST("/v1/images/edits", h.Images)
	tasks := service.NewImageTaskServiceWithUploader(&asyncImageMemoryStore{tasks: make(map[string]*service.ImageTaskRecord)}, nil, time.Hour, time.Minute)
	async := NewAsyncImageHandler(tasks, h)
	router.POST("/v1/images/generations/async", func(c *gin.Context) {
		asyncKey := *key
		asyncKey.UserID = 1701
		asyncKey.Group = &service.Group{ID: groupID, Platform: service.PlatformOpenAI, AllowImageGeneration: true}
		c.Set(string(middleware.ContextKeyAPIKey), &asyncKey)
		async.Submit(c)
	})
	router.GET("/v1/images/tasks/:task_id", func(c *gin.Context) {
		asyncKey := *key
		asyncKey.UserID = 1701
		c.Set(string(middleware.ContextKeyAPIKey), &asyncKey)
		async.Get(c)
	})
	router.POST("/v1/responses/input_tokens", h.ResponsesInputTokens)
	router.POST("/v1/messages/count_tokens", h.CountTokens)
	router.POST("/v1/alpha/search", func(c *gin.Context) {
		searchKey := *key
		searchKey.Group = &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive}
		c.Set(string(middleware.ContextKeyAPIKey), &searchKey)
		h.AlphaSearch(c)
	})
	server := httptest.NewServer(pkghttputil.WithWriteTimeout(router, 0))
	t.Cleanup(server.Close)
	return h, repo, cache, server
}

func awaitDisconnectSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func TestForceDisconnectRealHTTPHandlers(t *testing.T) {
	for _, tc := range []struct{ name, path, body string }{
		{"headers_pending", "/v1/responses", `{"model":"gpt-5.1","input":"hi","stream":true}`},
		{"responses_sse_payload", "/v1/responses", `{"model":"gpt-5.1","input":"hi","stream":true}`},
		{"responses_root_alias", "/responses", `{"model":"gpt-5.1","input":"hi","stream":true}`},
		{"responses_codex_alias", "/backend-api/codex/responses", `{"model":"gpt-5.1","input":"hi","stream":true}`},
		{"compact", "/v1/responses/compact", `{"model":"gpt-5.1","input":"hi"}`},
		{"compact_root_alias", "/responses/compact", `{"model":"gpt-5.1","input":"hi"}`},
		{"compact_codex_alias", "/backend-api/codex/responses/compact", `{"model":"gpt-5.1","input":"hi"}`},
		{"images_edits_multipart", "/v1/images/edits", "multipart"},
		{"responses_stream", "/v1/responses", `{"model":"gpt-5.1","input":"hi","stream":true}`},
		{"oauth_responses", "/v1/responses", `{"model":"gpt-5.1","input":"hi","stream":true}`},
		{"alpha_search", "/v1/alpha/search", `{"model":"gpt-5.1","query":"hi"}`},
		{"responses_nonstream", "/v1/responses", `{"model":"gpt-5.1","input":"hi"}`},
		{"chat", "/v1/chat/completions", `{"model":"gpt-5.1","messages":[{"role":"user","content":"hi"}],"stream":true}`},
		{"messages", "/v1/messages", `{"model":"gpt-5.1","messages":[{"role":"user","content":"hi"}],"max_tokens":20,"stream":true}`},
		{"embeddings", "/v1/embeddings", `{"model":"text-embedding-3-small","input":"hi"}`},
		{"images", "/v1/images/generations", `{"model":"gpt-image-1","prompt":"a tree","n":1}`},
		{"count_tokens", "/v1/messages/count_tokens", `{"model":"gpt-5.1","messages":[{"role":"user","content":"hi"}]}`},
		{"input_tokens", "/v1/responses/input_tokens", `{"model":"gpt-5.1","input":"hi"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, stopped := make(chan struct{}), make(chan struct{})
			firstPayload := make(chan struct{})
			var upstreamCalls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if upstreamCalls.Add(1) != 1 {
					t.Error("cancelled request was replayed")
					return
				}
				defer close(stopped)
				_, _ = io.Copy(io.Discard, r.Body)
				if tc.name == "headers_pending" {
					close(started)
					<-r.Context().Done()
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				if tc.name == "responses_sse_payload" {
					_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_live\",\"model\":\"gpt-5.1\"}}\n\n")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_live\",\"output_index\":0,\"content_index\":0,\"delta\":\"partial\"}\n\n")
					w.(http.Flusher).Flush()
				}
				close(started)
				<-r.Context().Done()
			}))
			defer upstream.Close()
			h, repo, cache, server := newDisconnectHandler(t, upstream.URL, "")
			if tc.name == "oauth_responses" {
				repo.account.Type = service.AccountTypeOAuth
				repo.account.Credentials["access_token"] = "test-oauth-token"
			}
			if tc.name == "input_tokens" || tc.name == "count_tokens" {
				repo.account.Credentials["base_url"] = "https://api.openai.com"
			}
			defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)
			clientDone := make(chan struct{})
			clientError := make(chan error, 1)
			clientCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			request := func(ctx context.Context) *http.Request {
				var body io.Reader = strings.NewReader(tc.body)
				contentType := "application/json"
				if tc.body == "multipart" {
					var form bytes.Buffer
					writer := multipart.NewWriter(&form)
					require.NoError(t, writer.WriteField("model", "gpt-image-1"))
					require.NoError(t, writer.WriteField("prompt", "edit tree"))
					part, err := writer.CreateFormFile("image", "input.png")
					require.NoError(t, err)
					_, err = part.Write([]byte("fixture image"))
					require.NoError(t, err)
					require.NoError(t, writer.Close())
					body = &form
					contentType = writer.FormDataContentType()
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+tc.path, body)
				require.NoError(t, err)
				req.Header.Set("Content-Type", contentType)
				return req
			}
			req := request(clientCtx)
			go func() {
				defer close(clientDone)
				res, err := server.Client().Do(req)
				if err == nil {
					defer res.Body.Close()
					reader := bufio.NewReader(res.Body)
					if tc.name == "responses_sse_payload" {
						_, err = reader.ReadString('\n')
						if err == nil {
							close(firstPayload)
						}
					}
					if err == nil {
						_, err = io.Copy(io.Discard, reader)
					}
				}
				clientError <- err
			}()
			awaitDisconnectSignal(t, started, "HTTP upstream request")
			if tc.name == "responses_sse_payload" {
				awaitDisconnectSignal(t, firstPayload, "SSE payload at downstream")
			}

			result, err := h.gatewayService.DisconnectAccount(context.Background(), 9901, 3*time.Second)
			require.NoError(t, err)
			require.True(t, result.Drained, "%+v", result)
			require.Equal(t, 1, result.CancellationRequested.HTTP)
			require.False(t, repo.schedulingEnabled.Load())
			awaitDisconnectSignal(t, stopped, "upstream cancellation")
			awaitDisconnectSignal(t, clientDone, "HTTP client completion")
			require.Error(t, <-clientError, "interrupted request must not appear as an empty successful response")
			require.NoError(t, clientCtx.Err(), "disconnect must precede the client timeout")
			require.Zero(t, h.gatewayService.SnapshotOpenAIAccountSchedulerMetrics().RuntimeStatsAccountCount, "administrative cancellation must not be reported as an upstream result")
			if tc.name != "input_tokens" && tc.name != "count_tokens" {
				cache.assertReleasedOnce(t)
			}
			// The scheduler stub intentionally still advertises the disabled account.
			retry := request(clientCtx)
			response, err := server.Client().Do(retry)
			require.NoError(t, err)
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			require.GreaterOrEqual(t, response.StatusCode, 400)
			require.Equal(t, int32(1), upstreamCalls.Load())
		})
	}
}

func TestForceDisconnectRealWebSocketActiveAndIdle(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool, service.OpenAIWSIngressModeDedicated, service.OpenAIWSIngressModeHTTPBridge} {
		for _, idle := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/idle=%t", mode, idle), func(t *testing.T) {
				started, stopped := make(chan struct{}), make(chan struct{})
				var upstreamCalls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if upstreamCalls.Add(1) != 1 {
						t.Error("disabled account received a new WS turn")
						return
					}
					defer close(stopped)
					completed := `{"type":"response.completed","response":{"id":"resp_force","model":"gpt-5.1","usage":{"input_tokens":2,"output_tokens":1}}}`
					if mode == service.OpenAIWSIngressModeHTTPBridge {
						_, _ = io.Copy(io.Discard, r.Body)
						w.Header().Set("Content-Type", "text/event-stream")
						w.WriteHeader(http.StatusOK)
						w.(http.Flusher).Flush()
						close(started)
						if idle {
							_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", completed)
							w.(http.Flusher).Flush()
							return
						}
						<-r.Context().Done()
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
					if _, _, err = conn.Read(ctx); err != nil {
						t.Error(err)
						return
					}
					close(started)
					if idle {
						if err = conn.Write(ctx, coderws.MessageText, []byte(completed)); err != nil {
							t.Error(err)
							return
						}
					}
					_, _, _ = conn.Read(ctx)
				}))
				defer upstream.Close()
				h, repo, cache, server := newDisconnectHandler(t, upstream.URL, mode)
				defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses", nil)
				require.NoError(t, err)
				defer conn.CloseNow()
				require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","input":"hi","stream":true}`)))
				awaitDisconnectSignal(t, started, "WebSocket first turn")
				if idle {
					for {
						_, payload, readErr := conn.Read(ctx)
						require.NoError(t, readErr)
						if strings.Contains(string(payload), `"response.completed"`) {
							break
						}
					}
					require.Eventually(t, func() bool {
						counts, _ := cache.GetAccountConcurrencyBatch(ctx, []int64{9901})
						return counts[9901] == 0
					}, time.Second, time.Millisecond)
				}
				result, err := h.gatewayService.DisconnectAccount(context.Background(), 9901, 3*time.Second)
				require.NoError(t, err)
				require.True(t, result.Drained, "%+v", result)
				require.Equal(t, 1, result.CancellationRequested.WebSockets, "idle connections must remain registered even with zero concurrency")
				require.False(t, repo.schedulingEnabled.Load())
				_, _, err = conn.Read(ctx)
				require.Error(t, err, "downstream WebSocket remained open")
				require.NoError(t, ctx.Err(), "must terminate before the test deadline")
				awaitDisconnectSignal(t, stopped, "upstream connection abort")
				cache.assertReleasedOnce(t)
				// A new connection still sees the intentionally stale scheduler
				// snapshot, but must fail the authoritative admission gate.
				next, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses", nil)
				require.NoError(t, err)
				defer next.CloseNow()
				require.NoError(t, next.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","input":"again"}`)))
				_, _, err = next.Read(ctx)
				require.Error(t, err)
				require.NoError(t, ctx.Err())
				require.Equal(t, int32(1), upstreamCalls.Load())
			})
		}
	}
}

func TestForceDisconnectUnresponsiveWebSocketUpstream(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool} {
		for _, idle := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/idle=%t", mode, idle), func(t *testing.T) {
				started := make(chan struct{})
				releasePeer := make(chan struct{})
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := coderws.Accept(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.CloseNow()
					ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
					defer cancel()
					if _, _, err = conn.Read(ctx); err != nil {
						t.Error(err)
						return
					}
					close(started)
					if idle {
						_ = conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_unresponsive","model":"gpt-5.1","usage":{"input_tokens":2,"output_tokens":1}}}`))
					}
					// Never read again: this peer cannot acknowledge any close handshake.
					<-releasePeer
				}))
				defer upstream.Close()
				defer close(releasePeer)
				h, _, cache, server := newDisconnectHandler(t, upstream.URL, mode)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses", nil)
				require.NoError(t, err)
				defer client.CloseNow()
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","input":"hi"}`)))
				awaitDisconnectSignal(t, started, "unresponsive upstream admission")
				if idle {
					_, _, err = client.Read(ctx)
					require.NoError(t, err)
				}
				begin := time.Now()
				result, err := h.gatewayService.DisconnectAccount(ctx, 9901, time.Second)
				require.NoError(t, err)
				require.True(t, result.Drained, "%+v", result)
				require.Less(t, time.Since(begin), 2*time.Second, "must bypass the five-second close handshake")
				_, _, err = client.Read(ctx)
				require.Error(t, err)
				cache.assertReleasedOnce(t)
			})
		}
	}
}

func TestForceDisconnectAsyncImageContextDoesNotOwnSubmissionResponse(t *testing.T) {
	router := gin.New()
	router.POST("/images/generations/async", func(c *gin.Context) {
		task, _, cancel, err := newAsyncImageContext(c, []byte(`{"model":"gpt-image-1","prompt":"tree"}`), time.Minute)
		if err != nil {
			t.Error(err)
			return
		}
		defer cancel()
		pkghttputil.InterruptHTTPResponse(task.Request.Context())
		c.JSON(http.StatusAccepted, gin.H{"status": "processing"})
	})
	server := httptest.NewServer(pkghttputil.WithWriteTimeout(router, 0))
	defer server.Close()
	res, err := server.Client().Post(server.URL+"/images/generations/async", "application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	require.JSONEq(t, `{"status":"processing"}`, string(body))
}

func TestForceDisconnectRetrySelectsEligibleBackupWithStickyRedis(t *testing.T) {
	for _, tc := range []struct{ name, path, mode string }{
		{"http", "/v1/responses", ""},
		{"ws_root", "/responses", service.OpenAIWSIngressModePassthrough},
		{"ws_codex", "/backend-api/codex/responses", service.OpenAIWSIngressModeCtxPool},
		{"ws_v1_dedicated", "/v1/responses", service.OpenAIWSIngressModeDedicated},
		{"ws_v1_http_bridge", "/v1/responses", service.OpenAIWSIngressModeHTTPBridge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			var aCalls, bCalls atomic.Int32
			completed := `{"type":"response.completed","response":{"id":"resp_backup","status":"completed","model":"gpt-5.1","output":[],"usage":{"input_tokens":2,"output_tokens":1}}}`
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				backup := r.Header.Get("Authorization") == "Bearer sk-backup"
				if backup {
					bCalls.Add(1)
				} else if aCalls.Add(1) != 1 {
					t.Error("disabled A received another upstream request")
					return
				}
				if tc.mode != "" && tc.mode != service.OpenAIWSIngressModeHTTPBridge {
					conn, err := coderws.Accept(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.CloseNow()
					if _, _, err = conn.Read(r.Context()); err != nil {
						t.Error(err)
						return
					}
					if backup {
						_ = conn.Write(r.Context(), coderws.MessageText, []byte(completed))
					} else {
						close(started)
					}
					_, _, _ = conn.Read(r.Context())
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				if backup {
					_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", completed)
					return
				}
				close(started)
				<-r.Context().Done()
			}))
			defer upstream.Close()
			redisServer := miniredis.RunT(t)
			redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
			defer redisClient.Close()
			cache := repository.NewConcurrencyCache(redisClient, 15, 900)
			sticky := repository.NewGatewayCache(redisClient)
			h, repo, _, server := newDisconnectHandler(t, upstream.URL, tc.mode, disconnectHandlerOptions{cache: cache, sticky: sticky, backup: true})
			defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			payload := `{"type":"response.create","model":"gpt-5.1","input":"hi","stream":true,"prompt_cache_key":"same-conversation"}`
			fake, _ := gin.CreateTestContext(httptest.NewRecorder())
			fake.Request = httptest.NewRequest(http.MethodPost, tc.path, nil)
			hash := h.gatewayService.GenerateSessionHash(fake, []byte(payload))
			require.NotEmpty(t, hash)
			groupID := int64(4201)
			require.NoError(t, h.gatewayService.BindStickySession(ctx, &groupID, hash, 9901))
			var conn *coderws.Conn
			clientErr := make(chan error, 1)
			url := server.URL + tc.path
			if tc.mode != "" {
				var err error
				conn, _, err = coderws.Dial(ctx, "ws"+strings.TrimPrefix(url, "http"), nil)
				require.NoError(t, err)
				defer conn.CloseNow()
				require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(payload)))
			} else {
				go func() {
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(payload))
					req.Header.Set("Content-Type", "application/json")
					res, err := server.Client().Do(req)
					if err == nil {
						_, err = io.Copy(io.Discard, res.Body)
						res.Body.Close()
					}
					clientErr <- err
				}()
			}
			awaitDisconnectSignal(t, started, "account A admitted")
			result, err := h.gatewayService.DisconnectAccount(ctx, 9901, 3*time.Second)
			require.NoError(t, err)
			require.True(t, result.Drained, "%+v", result)
			require.False(t, repo.schedulingEnabled.Load())
			if conn != nil {
				_, _, err = conn.Read(ctx)
				require.Error(t, err)
			} else {
				require.Error(t, <-clientErr)
			}
			// The repository still advertises A; sticky state also originated at A.
			// A new client attempt must pass the authoritative gate and use B.
			if tc.mode != "" {
				next, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(url, "http"), nil)
				require.NoError(t, err)
				defer next.CloseNow()
				require.NoError(t, next.Write(ctx, coderws.MessageText, []byte(payload)))
				for {
					_, frame, readErr := next.Read(ctx)
					require.NoError(t, readErr)
					if strings.Contains(string(frame), "resp_backup") {
						break
					}
				}
				_ = next.CloseNow()
			} else {
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(payload))
				req.Header.Set("Content-Type", "application/json")
				res, err := server.Client().Do(req)
				require.NoError(t, err)
				defer res.Body.Close()
				body, err := io.ReadAll(res.Body)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, res.StatusCode)
				require.Contains(t, string(body), "resp_backup")
			}
			require.Equal(t, int32(1), aCalls.Load())
			require.Equal(t, int32(1), bCalls.Load())
			counts, err := cache.GetAccountConcurrencyBatch(ctx, []int64{9901})
			require.NoError(t, err)
			require.Zero(t, counts[9901])
			require.Eventually(t, func() bool { n, e := cache.GetUserConcurrency(ctx, 1701); return e == nil && n == 0 }, time.Second, 5*time.Millisecond)
		})
	}
}

func TestForceDisconnectStopsAsyncImageUpstream(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(stopped)
	}))
	defer upstream.Close()
	h, _, cache, server := newDisconnectHandler(t, upstream.URL, "")
	defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)
	res, err := server.Client().Post(server.URL+"/v1/images/generations/async", "application/json", strings.NewReader(`{"model":"gpt-image-1","prompt":"tree"}`))
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	var accepted struct {
		PollURL string `json:"poll_url"`
	}
	require.NoError(t, json.NewDecoder(res.Body).Decode(&accepted))
	require.NotEmpty(t, accepted.PollURL)
	awaitDisconnectSignal(t, started, "async image upstream")
	result, err := h.gatewayService.DisconnectAccount(context.Background(), 9901, 3*time.Second)
	require.NoError(t, err)
	require.True(t, result.Drained, "%+v", result)
	awaitDisconnectSignal(t, stopped, "async image cancellation")
	require.Eventually(t, func() bool {
		poll, err := server.Client().Get(server.URL + accepted.PollURL)
		if err != nil {
			return false
		}
		defer poll.Body.Close()
		var task service.ImageTaskRecord
		return json.NewDecoder(poll.Body).Decode(&task) == nil && task.Status == service.ImageTaskStatusFailed
	}, time.Second, 10*time.Millisecond)
	cache.assertReleasedOnce(t)
}

func TestForceDisconnectCancelsAccountQueueWithoutDeletingForeignSlot(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer client.Close()
	cache := repository.NewConcurrencyCache(client, 15, 900)
	h, repo, _, _ := newDisconnectHandler(t, "http://127.0.0.1:1", "", disconnectHandlerOptions{cache: cache})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	acquired, err := cache.AcquireAccountSlot(ctx, 9901, 1, "other-instance-request")
	require.NoError(t, err)
	require.True(t, acquired)
	defer cache.ReleaseAccountSlot(context.Background(), 9901, "other-instance-request")
	router := gin.New()
	var forwarded atomic.Bool
	router.POST("/queued", func(c *gin.Context) {
		started := false
		selection := &service.AccountSelectionResult{Account: &repo.account, WaitPlan: &service.AccountWaitPlan{MaxConcurrency: 1, MaxWaiting: 5, Timeout: time.Minute}}
		release, result := h.acquireOpenAIAccountSlot(c, nil, "", selection, false, &started, zap.NewNop(), nil)
		if result == openAISlotAcquireOK {
			defer release()
			forwarded.Store(true)
		}
	})
	server := httptest.NewServer(pkghttputil.WithWriteTimeout(router, 0))
	defer server.Close()
	clientError := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/queued", nil)
		res, err := server.Client().Do(req)
		if err == nil {
			_, err = io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}
		clientError <- err
	}()
	require.Eventually(t, func() bool { count, err := cache.GetAccountWaitingCount(ctx, 9901); return err == nil && count == 1 }, time.Second, 5*time.Millisecond)
	result, err := h.gatewayService.DisconnectAccount(ctx, 9901, 300*time.Millisecond)
	require.NoError(t, err)
	require.False(t, result.Drained)
	require.True(t, result.TimedOut)
	require.Equal(t, 1, result.CancellationRequested.HTTP)
	require.Equal(t, service.AccountRequestCounts{}, result.Remaining)
	require.Equal(t, 1, *result.CurrentConcurrency, "a foreign request slot cannot be erased to fake completion")
	require.Error(t, <-clientError)
	require.NoError(t, ctx.Err())
	require.False(t, forwarded.Load())
	count, err := cache.GetAccountWaitingCount(ctx, 9901)
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, cache.ReleaseAccountSlot(ctx, 9901, "other-instance-request"))
	result, err = h.gatewayService.DisconnectAccount(ctx, 9901, time.Second)
	require.NoError(t, err)
	require.True(t, result.Drained)
}

func TestForceDisconnectDoesNotInterruptOtherAccountStream(t *testing.T) {
	aStarted, bStarted, releaseB := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseB) })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		if r.Header.Get("Authorization") == "Bearer sk-backup" {
			close(bStarted)
			select {
			case <-releaseB:
			case <-r.Context().Done():
				t.Error("B was interrupted by disconnecting A")
				return
			}
			_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_B_survived\",\"status\":\"completed\",\"model\":\"gpt-5.1\",\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\ndata: [DONE]\n\n")
			return
		}
		close(aStarted)
		<-r.Context().Done()
	}))
	defer upstream.Close()
	defer releaseOnce.Do(func() { close(releaseB) })
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer redisClient.Close()
	cache := repository.NewConcurrencyCache(redisClient, 15, 900)
	h, _, _, server := newDisconnectHandler(t, upstream.URL, "", disconnectHandlerOptions{cache: cache, sticky: repository.NewGatewayCache(redisClient), backup: true})
	defer h.gatewayService.DisconnectAccount(context.Background(), 9901, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type outcome struct {
		body string
		err  error
	}
	request := func(session string, id int64) chan outcome {
		payload := fmt.Sprintf(`{"model":"gpt-5.1","input":"hi","stream":true,"prompt_cache_key":"%s"}`, session)
		fake, _ := gin.CreateTestContext(httptest.NewRecorder())
		fake.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		hash := h.gatewayService.GenerateSessionHash(fake, []byte(payload))
		groupID := int64(4201)
		require.NoError(t, h.gatewayService.BindStickySession(ctx, &groupID, hash, id))
		done := make(chan outcome, 1)
		go func() {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			res, err := server.Client().Do(req)
			var body []byte
			if err == nil {
				body, err = io.ReadAll(res.Body)
				res.Body.Close()
			}
			done <- outcome{string(body), err}
		}()
		return done
	}
	aDone := request("isolate-A", 9901)
	awaitDisconnectSignal(t, aStarted, "A started")
	bDone := request("isolate-B", 9902)
	awaitDisconnectSignal(t, bStarted, "B started")
	result, err := h.gatewayService.DisconnectAccount(ctx, 9901, 3*time.Second)
	require.NoError(t, err)
	require.True(t, result.Drained, "%+v", result)
	require.Error(t, (<-aDone).err)
	counts, err := cache.GetAccountConcurrencyBatch(ctx, []int64{9901, 9902})
	require.NoError(t, err)
	require.Zero(t, counts[9901])
	require.Equal(t, 1, counts[9902])
	select {
	case got := <-bDone:
		t.Fatalf("B completed prematurely: %+v", got)
	default:
	}
	releaseOnce.Do(func() { close(releaseB) })
	got := <-bDone
	require.NoError(t, got.err)
	require.Contains(t, got.body, "resp_B_survived")
	require.Eventually(t, func() bool { n, e := cache.GetUserConcurrency(ctx, 1701); return e == nil && n == 0 }, time.Second, 5*time.Millisecond)
}
