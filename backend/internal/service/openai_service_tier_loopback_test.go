package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Real loopback HTTP, with the production transport boundary redirected to a
// local fixture. No provider credentials, public network or production billing.
type serviceTierLoopbackUpstream struct {
	client *http.Client
	target *url.URL
}

func (u *serviceTierLoopbackUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	local := req.Clone(req.Context())
	local.URL.Scheme, local.URL.Host = u.target.Scheme, u.target.Host
	local.Host = u.target.Host
	return u.client.Do(local)
}

func (u *serviceTierLoopbackUpstream) DoWithTLS(req *http.Request, proxy string, accountID int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxy, accountID, concurrency)
}

func newServiceTierLoopbackGateway(t *testing.T, target string) *OpenAIGatewayService {
	t.Helper()
	parsed, err := url.Parse(target)
	require.NoError(t, err)
	cfg := &config.Config{Security: config.SecurityConfig{ResponseHeaders: config.ResponseHeaderConfig{
		Enabled: true, ForceRemove: []string{"content-type"},
	}}}
	return &OpenAIGatewayService{
		cfg: cfg, responseHeaderFilter: compileResponseHeaderFilter(cfg), cache: &stubGatewayCache{},
		httpUpstream: &serviceTierLoopbackUpstream{client: &http.Client{Timeout: 5 * time.Second}, target: parsed},
	}
}

// Assert the actual RecordUsage path, including token bucket separation,
// pricing, persisted usage arguments and the user balance deduction. The
// injected repositories capture effects without writing a real database.
func recordServiceTierLoopbackUsage(t *testing.T, result *OpenAIForwardResult, account *Account, rate float64, freeFast bool) *UsageLog {
	t.Helper()
	logs := &openAIRecordUsageLogRepoStub{inserted: true}
	users := &openAIRecordUsageUserRepoStub{}
	svc := newOpenAIRecordUsageServiceForTest(logs, users, &openAIRecordUsageSubRepoStub{}, nil)
	svc.resolver = NewModelPricingResolver(nil, svc.billingService)
	groupID := int64(77)
	key := &APIKey{ID: 1020, GroupID: &groupID, Group: &Group{
		ID: groupID, Platform: PlatformOpenAI, Status: StatusActive, Hydrated: true,
		RateMultiplier: rate, FreeOpenAIFast: freeFast,
		ModelPricing: []ChannelModelPricing{{
			Models: []string{"gpt-5.5"}, BillingMode: BillingModeToken,
			InputPrice: f64p(1e-6), OutputPrice: f64p(2e-6), CacheReadPrice: f64p(0.1e-6),
			FastMultiplier: f64p(2), FlexMultiplier: f64p(0.5), UltrafastMultiplier: f64p(6),
		}},
	}}
	require.NoError(t, svc.RecordUsage(context.Background(), &OpenAIRecordUsageInput{
		Result: result, Account: account, APIKey: key, User: &User{ID: 2020},
	}))
	require.Equal(t, 1, logs.calls)
	require.Equal(t, 1, users.deductCalls)
	require.NotNil(t, logs.lastLog)
	require.InDelta(t, logs.lastLog.ActualCost, users.lastAmount, 1e-12)
	require.Equal(t, 1345, logs.lastLog.InputTokens)
	require.Equal(t, 81152, logs.lastLog.CacheReadTokens)
	require.Equal(t, 2176, logs.lastLog.OutputTokens)
	return logs.lastLog
}

func TestOpenAIServiceTierLoopbackBillsBothHops(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, stream := range []bool{false, true} {
		for _, passthrough := range []bool{false, true} {
			for _, tc := range []struct {
				name, accountType, requested, observed, billed string
				multiplier                                     float64
				freeFast                                       bool
			}{
				{"priority", AccountTypeOAuth, "priority", "default", "priority", 2, false},
				{"fast_alias", AccountTypeOAuth, "fast", "default", "priority", 2, false},
				{"missing_echo", AccountTypeOAuth, "priority", "", "priority", 2, false},
				{"auto_echo", AccountTypeOAuth, "priority", "auto", "priority", 2, false},
				{"ultrafast", AccountTypeOAuth, "ultrafast", "default", "ultrafast", 6, false},
				{"real_flex_downgrade", AccountTypeOAuth, "priority", "flex", "flex", 0.5, false},
				{"requested_flex", AccountTypeOAuth, "flex", "default", "flex", 0.5, false},
				{"standard", AccountTypeOAuth, "default", "default", "default", 1, false},
				{"omitted", AccountTypeOAuth, "", "default", "", 1, false},
				{"public_api_downgrade", AccountTypeAPIKey, "priority", "default", "default", 1, false},
				{"outer_free_fast", AccountTypeOAuth, "priority", "default", "priority", 2, true},
				{"ultrafast_not_free", AccountTypeOAuth, "ultrafast", "default", "ultrafast", 6, true},
			} {
				t.Run(fmt.Sprintf("%s/stream=%t/passthrough=%t", tc.name, stream, passthrough), func(t *testing.T) {
					outbound := make(chan string, 1)
					provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							http.Error(w, err.Error(), 400)
							return
						}
						outbound <- gjson.GetBytes(body, "service_tier").String()
						tier := ""
						if tc.observed != "" {
							tier = fmt.Sprintf(`,"service_tier":%q`, tc.observed)
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_loopback\",\"model\":\"gpt-5.5\"%s}}\n\n", tier)
						w.(http.Flusher).Flush()
						_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_loopback\",\"object\":\"response\",\"model\":\"gpt-5.5\",\"status\":\"completed\"%s,\"output\":[],\"usage\":{\"input_tokens\":82497,\"output_tokens\":2176,\"input_tokens_details\":{\"cached_tokens\":81152}}}}\n\ndata: [DONE]\n\n", tier)
					}))
					defer provider.Close()
					innerAccount := &Account{ID: 1, Platform: PlatformOpenAI, Type: tc.accountType, Concurrency: 1,
						Credentials: map[string]any{"access_token": "fixture-token", "chatgpt_account_id": "fixture-account", "api_key": "fixture-key"},
						Extra:       map[string]any{"openai_responses_supported": true, "openai_passthrough": passthrough}}
					innerSvc := newServiceTierLoopbackGateway(t, provider.URL)
					type outcome struct {
						result *OpenAIForwardResult
						err    error
					}
					finished := make(chan outcome, 1)
					relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							finished <- outcome{err: err}
							http.Error(w, err.Error(), 400)
							return
						}
						c, _ := gin.CreateTestContext(w)
						c.Request = r
						result, err := innerSvc.Forward(r.Context(), c, innerAccount, body)
						finished <- outcome{result, err}
					}))
					defer relay.Close()
					outerAccount := &Account{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1,
						Credentials: map[string]any{"api_key": "fixture-key"}, Extra: map[string]any{"openai_responses_supported": true}}
					outerSvc := newServiceTierLoopbackGateway(t, relay.URL)
					tier := ""
					if tc.requested != "" {
						tier = fmt.Sprintf(`,"service_tier":%q`, tc.requested)
					}
					body := []byte(fmt.Sprintf(`{"model":"gpt-5.5","stream":%t,"input":"hello"%s}`, stream, tier))
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
					outer, err := outerSvc.Forward(context.Background(), c, outerAccount, body)
					require.NoError(t, err)
					require.Equal(t, 200, rec.Code)
					var inner *OpenAIForwardResult
					select {
					case got := <-finished:
						require.NoError(t, got.err)
						inner = got.result
					case <-time.After(time.Second):
						t.Fatal("inner forwarding did not finish")
					}
					require.NotNil(t, inner)
					wantOutbound := tc.requested
					if wantOutbound == "fast" {
						wantOutbound = "priority"
					}
					require.Equal(t, wantOutbound, <-outbound)
					require.Equal(t, normalizeObservedOpenAIServiceTier(tc.observed), inner.UpstreamResponseServiceTier, "client normalization must not rewrite raw billing observation")
					require.Equal(t, inner.Usage, outer.Usage)
					innerLog := recordServiceTierLoopbackUsage(t, inner, innerAccount, 1, false)
					outerLog := recordServiceTierLoopbackUsage(t, outer, outerAccount, 0.5, tc.freeFast)
					require.Equal(t, tc.billed, optionalStringValue(innerLog.ServiceTier))
					require.Equal(t, tc.billed, optionalStringValue(outerLog.ServiceTier))
					standard := 1345*1e-6 + 81152*0.1e-6 + 2176*2e-6
					require.InDelta(t, standard*tc.multiplier, innerLog.TotalCost, 1e-12)
					require.InDelta(t, innerLog.TotalCost, innerLog.ActualCost, 1e-12)
					require.InDelta(t, innerLog.TotalCost, outerLog.TotalCost, 1e-12)
					charge := standard * tc.multiplier * 0.5
					if tc.freeFast && tc.billed == "priority" {
						charge = standard * 0.5
					}
					require.InDelta(t, charge, outerLog.ActualCost, 1e-12)
					require.NotEmpty(t, strings.TrimSpace(rec.Body.String()))
				})
			}
		}
	}
}
