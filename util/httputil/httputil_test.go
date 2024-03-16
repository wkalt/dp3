package httputil_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/httputil"
)

func TestBadRequest(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/foo", nil)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httputil.BadRequest(r.Context(), w, "bad request")
	})
	recorder := httptest.NewRecorder()
	handler(recorder, req)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	require.Equal(t, `{"error":"bad request"}`+"\n", recorder.Body.String())
}

func TestInternalServerError(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/foo", nil)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httputil.InternalServerError(r.Context(), w, "internal server error")
	})
	recorder := httptest.NewRecorder()
	handler(recorder, req)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	require.Equal(t, `{"error":"Internal server error"}`+"\n", recorder.Body.String())
}
