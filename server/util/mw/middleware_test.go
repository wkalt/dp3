package mw_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	glog "log"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/util/log"
	"github.com/wkalt/dp3/server/util/mw"
)

func TestWithRequestID(t *testing.T) {
	ctx := context.Background()
	buf := &bytes.Buffer{}
	glog.SetOutput(buf)
	defer func() {
		glog.SetOutput(os.Stderr)
	}()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Infof(r.Context(), "test")
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	middleware := mw.WithRequestID(handler)
	middleware.ServeHTTP(recorder, req)
	require.Contains(t, buf.String(), "request_id")
}

func TestWithCORSAllowedOrigins(t *testing.T) {
	ctx := context.Background()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("ok"))
		require.NoError(t, err)
	})

	t.Run("allowed", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
		require.NoError(t, err)
		req.Header.Set("Origin", "http://example.com")
		recorder := httptest.NewRecorder()
		middleware := mw.WithCORSAllowedOrigins([]string{"http://example.com"})(handler)
		middleware.ServeHTTP(recorder, req)
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Equal(t, "ok", recorder.Body.String())
		require.Equal(t, "http://example.com", recorder.Header().Get("Access-Control-Allow-Origin"))
	})
	t.Run("origin not allowed (still processes request)", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
		require.NoError(t, err)
		req.Header.Set("Origin", "http://example.com")
		recorder := httptest.NewRecorder()
		middleware := mw.WithCORSAllowedOrigins([]string{"http://example.org"})(handler)
		middleware.ServeHTTP(recorder, req)
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Equal(t, "ok", recorder.Body.String())
		require.Empty(t, recorder.Header().Get("Access-Control-Allow-Origin"))
	})
	t.Run("options request responds with header but skips request processing", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodOptions, "/", nil)
		require.NoError(t, err)
		req.Header.Set("Origin", "http://example.com")
		recorder := httptest.NewRecorder()
		middleware := mw.WithCORSAllowedOrigins([]string{"http://example.org"})(handler)
		middleware.ServeHTTP(recorder, req)
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Empty(t, recorder.Body.String())
		require.Empty(t, recorder.Header().Get("Access-Control-Allow-Origin"))
	})
}
