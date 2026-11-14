package handler

import (
	"context"
	"errors"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"go.uber.org/zap"
)

// checkOpenAIAccountAdmission releases only the just-acquired slot on refusal.
// Already forwarded requests keep their slots and finish normally.
func (h *OpenAIGatewayHandler) checkOpenAIAccountAdmission(ctx context.Context, accountID int64, release func(), reqLog *zap.Logger, writeError openAISlotErrorWriter) openAISlotAcquireResult {
	err := h.gatewayService.CheckAccountScheduling(ctx, accountID)
	if err == nil {
		return openAISlotAcquireOK
	}
	if release != nil {
		release()
	}
	if errors.Is(err, service.ErrAccountSchedulingDisabled) {
		reqLog.Debug("openai.account_admission_disabled", zap.Int64("account_id", accountID))
		return openAISlotAcquireSchedulingDisabled
	}
	reqLog.Warn("openai.account_admission_check_failed", zap.Int64("account_id", accountID), zap.Error(err))
	writeError(http.StatusServiceUnavailable, "api_error", "", "Unable to verify account scheduling state, please retry later")
	return openAISlotAcquireFailed
}

func isOpenAIAccountAdmissionError(err error) bool {
	return errors.Is(err, service.ErrAccountSchedulingDisabled) || errors.Is(err, service.ErrAccountSchedulingCheckFailed)
}
