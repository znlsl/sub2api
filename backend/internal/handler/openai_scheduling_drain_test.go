package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type admissionGateRepo struct {
	service.AccountRepository
	enabled atomic.Bool
	readErr error
}

func (r *admissionGateRepo) IsAccountSchedulingEnabled(context.Context, int64) (bool, error) {
	return r.enabled.Load(), r.readErr
}
func admissionTestGateway(repo service.AccountRepository) *service.OpenAIGatewayService {
	return service.NewOpenAIGatewayService(repo, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
}

func TestOpenAIAccountAdmission_DisabledAfterSelection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"scheduler_acquired", "quick_acquire", "queued_acquire"} {
		t.Run(path, func(t *testing.T) {
			repo := &admissionGateRepo{}
			repo.enabled.Store(true)
			var acquires atomic.Int32
			cache := &concurrencyCacheMock{acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) {
				n := acquires.Add(1)
				if path == "queued_acquire" && n == 1 {
					return false, nil
				}
				// Disable while the selected request is obtaining its slot. Its cached
				// account still says enabled, and no profit gate is installed.
				repo.enabled.Store(false)
				return true, nil
			}}
			h := &OpenAIGatewayHandler{gatewayService: admissionTestGateway(repo), concurrencyHelper: NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, 0)}
			selection := &service.AccountSelectionResult{Account: &service.Account{ID: 5579, Schedulable: true, Status: service.StatusActive}, WaitPlan: &service.AccountWaitPlan{AccountID: 5579, MaxConcurrency: 24, MaxWaiting: 4, Timeout: time.Second}}
			var directRelease atomic.Int32
			if path == "scheduler_acquired" {
				selection.Acquired = true
				selection.ReleaseFunc = func() { directRelease.Add(1) }
				repo.enabled.Store(false)
			}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			started := false
			release, result := h.acquireResponsesAccountSlot(c, nil, "", selection, false, &started, zap.NewNop())
			require.Equal(t, openAISlotAcquireSchedulingDisabled, result)
			require.Nil(t, release)
			require.Empty(t, w.Body.String(), "the caller must be able to select a different account")
			require.Equal(t, int32(1), directRelease.Load()+atomic.LoadInt32(&cache.releaseAccountCalled), "release the rejected slot exactly once")
			if path == "queued_acquire" {
				require.GreaterOrEqual(t, acquires.Load(), int32(2))
			}
		})
	}
}

func TestOpenAIAccountAdmission_ReadFailureDoesNotForward(t *testing.T) {
	repo := &admissionGateRepo{readErr: errors.New("database unavailable")}
	repo.enabled.Store(true)
	h := &OpenAIGatewayHandler{gatewayService: admissionTestGateway(repo)}
	var releases int
	var status int
	result := h.checkOpenAIAccountAdmission(context.Background(), 5579, func() { releases++ }, zap.NewNop(), func(s int, _, _, _ string) { status = s })
	require.Equal(t, openAISlotAcquireFailed, result)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, 1, releases)
}

func TestOpenAIAccountAdmission_DisablingDoesNotReleaseAdmittedRequest(t *testing.T) {
	repo := &admissionGateRepo{}
	repo.enabled.Store(true)
	h := &OpenAIGatewayHandler{gatewayService: admissionTestGateway(repo)}
	var releases int
	release := func() { releases++ }
	require.Equal(t, openAISlotAcquireOK, h.checkOpenAIAccountAdmission(context.Background(), 5579, release, zap.NewNop(), nil))
	repo.enabled.Store(false)
	require.Zero(t, releases, "the admitted request still owns its slot")
	require.Equal(t, openAISlotAcquireSchedulingDisabled, h.checkOpenAIAccountAdmission(context.Background(), 5579, func() {}, zap.NewNop(), nil))
	require.Zero(t, releases, "refusing the next request must not cancel the existing one")
	release()
	require.Equal(t, 1, releases)
	repo.enabled.Store(true)
	require.Equal(t, openAISlotAcquireOK, h.checkOpenAIAccountAdmission(context.Background(), 5579, nil, zap.NewNop(), nil))
}

func TestOpenAIResponsesWebSocket_DisableDrainsCurrentTurnAndRejectsNext(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool} {
		t.Run(mode, func(t *testing.T) {
			var enabled atomic.Bool
			enabled.Store(true)
			runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
				firstPayload: wsGroupPricingFirst, secondPayload: wsGroupPricingSecond,
				ingressMode: mode, schedulingEnabled: &enabled,
				afterFirstUpstreamRequest: func(*service.ChannelService) error {
					// The upstream has received turn 1 but has NOT sent its response yet.
					enabled.Store(false)
					return nil
				},
				secondTurnCloseExpected: true,
				closeStatus:             coderws.StatusTryAgainLater,
				closeReason:             "account is no longer schedulable",
			})
			// The harness requires response.completed for turn 1 and confirms only
			// one request reached upstream, before checking the close for turn 2.
		})
	}
}

func TestOpenAIAccountAdmission_RefusalIsNotAccountFailure(t *testing.T) {
	for _, cause := range []error{service.ErrAccountSchedulingDisabled, service.ErrAccountSchedulingCheckFailed} {
		err := service.NewOpenAIWSClientCloseError(coderws.StatusTryAgainLater, "please reconnect", cause)
		require.True(t, isOpenAIAccountAdmissionError(err))
		require.False(t, shouldReportOpenAIWSProxyAccountFailure(err))
	}
	require.True(t, shouldReportOpenAIWSProxyAccountFailure(errors.New("upstream connection reset")))
}
