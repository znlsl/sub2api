package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type disconnectStub struct {
	result *service.AccountDisconnectResult
	err    error
	calls  int
	id     int64
	wait   time.Duration
}

func (s *disconnectStub) DisconnectAccount(_ context.Context, id int64, wait time.Duration) (*service.AccountDisconnectResult, error) {
	s.calls++
	s.id = id
	s.wait = wait
	return s.result, s.err
}

func TestAccountDisconnectEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, body, id string
		result         *service.AccountDisconnectResult
		err            error
		status         int
		calls          int
		wait           time.Duration
	}{
		{name: "drained", body: `{"wait_timeout_ms":200,"reason":"candy_failed"}`, id: "42", result: &service.AccountDisconnectResult{AccountID: 42, Drained: true}, status: 200, calls: 1, wait: 200 * time.Millisecond},
		{name: "pending", body: `{"wait_timeout_ms":0}`, id: "42", result: &service.AccountDisconnectResult{AccountID: 42}, status: 202, calls: 1},
		{name: "empty body defaults", id: "42", result: &service.AccountDisconnectResult{AccountID: 42, Drained: true}, status: 200, calls: 1, wait: 5 * time.Second},
		{name: "unknown count", body: `{}`, id: "42", result: &service.AccountDisconnectResult{AccountID: 42}, err: service.ErrAccountDisconnectUnverified, status: 503, calls: 1, wait: 5 * time.Second},
		{name: "unsupported", body: `{}`, id: "42", err: service.ErrAccountDisconnectUnsupported, status: 400, calls: 1, wait: 5 * time.Second},
		{name: "not found", body: `{}`, id: "42", err: service.ErrAccountNotFound, status: 404, calls: 1, wait: 5 * time.Second},
		{name: "internal failure", body: `{}`, id: "42", err: errors.New("db failure"), status: 500, calls: 1, wait: 5 * time.Second},
		{name: "negative id", body: `{}`, id: "-1", status: 400},
		{name: "bad id", body: `{}`, id: "hello", status: 400},
		{name: "negative wait", body: `{"wait_timeout_ms":-1}`, id: "42", status: 400},
		{name: "long wait", body: `{"wait_timeout_ms":30001}`, id: "42", status: 400},
		{name: "unknown option", body: `{"force":true}`, id: "42", status: 400},
		{name: "multiple objects", body: `{} {}`, id: "42", status: 400},
		{name: "null body", body: `null`, id: "42", status: 400},
		{name: "oversized reason", body: `{"reason":"` + strings.Repeat("x", 257) + `"}`, id: "42", status: 400},
		{name: "oversized body", body: `{"reason":"` + strings.Repeat("x", 4096) + `"}`, id: "42", status: 400},
		{name: "invalid json", body: `{`, id: "42", status: 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &disconnectStub{result: tc.result, err: tc.err}
			handler := &AccountHandler{accountDisconnector: stub}
			router := gin.New()
			router.POST("/accounts/:id/disconnect", handler.Disconnect)
			request := httptest.NewRequest(http.MethodPost, "/accounts/"+tc.id+"/disconnect", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
			require.Equal(t, tc.calls, stub.calls)
			if tc.calls > 0 {
				require.Equal(t, int64(42), stub.id)
				require.Equal(t, tc.wait, stub.wait)
			}
			if tc.status == 503 {
				var payload struct {
					Data service.AccountDisconnectResult `json:"data"`
				}
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &payload))
				require.False(t, payload.Data.Drained)
				require.Nil(t, payload.Data.CurrentConcurrency)
			}
		})
	}
}

func TestAccountDisconnectRequiresAdminAuthentication(t *testing.T) {
	stub := &disconnectStub{}
	handler := &AccountHandler{accountDisconnector: stub}
	router := gin.New()
	admin := router.Group("/api/v1/admin", gin.HandlerFunc(middleware.NewAdminAuthMiddleware(nil, nil, nil, nil)))
	admin.POST("/accounts/:id/disconnect", handler.Disconnect)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/42/disconnect", strings.NewReader(`{}`)))
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Zero(t, stub.calls)
}
