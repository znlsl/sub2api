package httputil

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestForceCancellationSurvivesNestedDetachAndClientDrain(t *testing.T) {
	client, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	force, cancelForce := context.WithCancelCause(context.Background())
	sentinel := errors.New("operator disconnected account")
	parent := WithForceCancellation(client, force)
	parent = DetachUpstreamContext(DetachUpstreamContext(parent, time.Hour))
	attempt, release := NewUpstreamAttemptContext(parent, time.Hour)
	defer release()
	cancelClient()
	require.NoError(t, attempt.Err(), "ordinary disconnect retains bounded usage drain")
	cancelForce(sentinel)
	select {
	case <-attempt.Done():
	case <-time.After(time.Second):
		t.Fatal("forced cancel did not propagate")
	}
	require.ErrorIs(t, context.Cause(attempt), sentinel)
	require.ErrorIs(t, context.Cause(parent), sentinel)
	require.ErrorIs(t, parent.Err(), context.Canceled)
}

func TestForceCancellationStopsRealDetachedHTTPStream(t *testing.T) {
	upstreamClosed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(upstreamClosed)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: started\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	force, abort := context.WithCancelCause(context.Background())
	defer abort(context.Canceled)
	ctx := DetachUpstreamContext(WithForceCancellation(context.Background(), force))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	require.NoError(t, err)
	response, err := server.Client().Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	finished := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, response.Body); finished <- err }()
	abort(errors.New("forced"))
	select {
	case err := <-finished:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("HTTP body kept reading")
	}
	select {
	case <-upstreamClosed:
	case <-time.After(time.Second):
		t.Fatal("upstream request still connected")
	}
}

func TestForceHTTPResponseOnlyInterruptsSelectedHTTP2Stream(t *testing.T) {
	active := make(chan context.Context, 1)
	finished := make(chan error, 1)
	server := httptest.NewUnstartedServer(WithWriteTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test-Connection", r.RemoteAddr)
		if r.URL.Path == "/other" {
			_, _ = io.WriteString(w, "other account ok")
			return
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		active <- r.Context()
		block := make([]byte, 64<<10)
		for i := 0; i < 4096; i++ {
			if _, err := w.Write(block); err != nil {
				finished <- err
				return
			}
		}
		finished <- nil
	}), 0))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	client := server.Client()
	response, err := client.Get(server.URL + "/blocked")
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, 2, response.ProtoMajor)
	ctx := <-active
	InterruptHTTPResponse(ctx)
	select {
	case err := <-finished:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("blocked HTTP/2 writer did not exit")
	}
	other, err := client.Get(server.URL + "/other")
	require.NoError(t, err)
	defer other.Body.Close()
	require.Equal(t, response.Header.Get("X-Test-Connection"), other.Header.Get("X-Test-Connection"), "other stream should reuse the same HTTP/2 connection")
	body, err := io.ReadAll(other.Body)
	require.NoError(t, err)
	require.Equal(t, "other account ok", strings.TrimSpace(string(body)))
}

func TestForceCancellationAfterClientCancellation(t *testing.T) {
	client, cancelClient := context.WithCancel(context.Background())
	force, cancelForce := context.WithCancelCause(context.Background())
	defer cancelForce(context.Canceled)
	ctx := WithForceCancellation(client, force)
	upstream := DetachUpstreamContext(ctx)
	cancelClient()
	require.Nil(t, ForceCancellationCause(ctx))
	require.NoError(t, upstream.Err())
	sentinel := errors.New("operator disconnected after client left")
	cancelForce(sentinel)
	require.ErrorIs(t, ForceCancellationCause(ctx), sentinel)
	require.ErrorIs(t, context.Cause(upstream), sentinel)
}

func TestForceHTTPResponseIgnoresCompletedRequest(t *testing.T) {
	first := make(chan context.Context, 1)
	returned := make(chan struct{})
	wrapped := WithWriteTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test-Connection", r.RemoteAddr)
		if r.URL.Path == "/first" {
			first <- r.Context()
		}
		_, _ = io.WriteString(w, "ok")
	}), 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wrapped.ServeHTTP(w, r)
		if r.URL.Path == "/first" {
			close(returned)
		}
	}))
	defer server.Close()
	response, err := server.Client().Get(server.URL + "/first")
	require.NoError(t, err)
	_, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	response.Body.Close()
	<-returned
	InterruptHTTPResponse(<-first)
	next, err := server.Client().Get(server.URL + "/other")
	require.NoError(t, err)
	defer next.Body.Close()
	body, err := io.ReadAll(next.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(body))
	require.Equal(t, response.Header.Get("X-Test-Connection"), next.Header.Get("X-Test-Connection"))
}
