package handler

import (
	"context"
	"errors"

	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func accountRequestWasDisconnected(ctx context.Context) bool {
	return errors.Is(pkghttputil.ForceCancellationCause(ctx), service.ErrAccountRequestsDisconnected)
}

// Register before admission, including queue waits. Finish only when the
// forwarding call has unwound, even if context cancellation released a slot early.
func (h *OpenAIGatewayHandler) beginOpenAIAccountHTTPRequest(c *gin.Context, accountID int64) (func(), error) {
	original := c.Request.Context()
	ctx, finish, err := h.gatewayService.BeginAccountRequest(original, accountID, service.AccountRequestHTTP,
		func() { pkghttputil.InterruptHTTPResponse(original) })
	if err != nil {
		return nil, err
	}
	c.Request = c.Request.WithContext(ctx)
	return finish, nil
}
