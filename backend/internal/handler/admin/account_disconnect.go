package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type accountDisconnector interface {
	DisconnectAccount(context.Context, int64, time.Duration) (*service.AccountDisconnectResult, error)
}

type disconnectAccountRequest struct {
	WaitTimeoutMS *int   `json:"wait_timeout_ms"`
	Reason        string `json:"reason"`
}

// Disconnect is separate from the normal graceful scheduling toggle.
// Authentication is inherited from the admin route group.
func (h *AccountHandler) Disconnect(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	var request disconnectAccountRequest
	if c.Request.Body != nil {
		decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 4096))
		decoder.DisallowUnknownFields()
		var parsed *disconnectAccountRequest
		err := decoder.Decode(&parsed)
		if (err != nil && !errors.Is(err, io.EOF)) || (err == nil && parsed == nil) {
			response.BadRequest(c, "Invalid disconnect request")
			return
		}
		if parsed != nil {
			request = *parsed
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			response.BadRequest(c, "Expected a single JSON object")
			return
		}
	}
	wait := 5000
	if request.WaitTimeoutMS != nil {
		wait = *request.WaitTimeoutMS
	}
	if wait < 0 || wait > 30000 || len(request.Reason) > 256 {
		response.BadRequest(c, "wait_timeout_ms must be 0..30000 and reason at most 256 bytes")
		return
	}
	if h.accountDisconnector == nil {
		response.Error(c, http.StatusServiceUnavailable, "Account disconnect service unavailable")
		return
	}
	result, err := h.accountDisconnector.DisconnectAccount(c.Request.Context(), id, time.Duration(wait)*time.Millisecond)
	if errors.Is(err, service.ErrAccountDisconnectUnsupported) {
		response.BadRequest(c, err.Error())
		return
	}
	if errors.Is(err, service.ErrAccountDisconnectUnverified) {
		c.JSON(http.StatusServiceUnavailable, response.Response{Code: http.StatusServiceUnavailable,
			Message: "Cancellation requested; completion could not be verified", Data: result})
		return
	}
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	slog.Info("admin.account_disconnect", "account_id", id, "reason", request.Reason,
		"drained", result.Drained, "remaining_http", result.Remaining.HTTP, "remaining_websockets", result.Remaining.WebSockets)
	if result.Drained {
		response.Success(c, result)
	} else {
		response.Accepted(c, result)
	}
}
