package httputil_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/util/httputil"
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

type e struct{}

func (e) Error() string  { return "bad request" }
func (e) Detail() string { return "helpful details" }

func TestErrorDetailing(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/foo", nil)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := e{}
		httputil.BadRequest(r.Context(), w, "%w", err)
	})
	recorder := httptest.NewRecorder()
	handler(recorder, req)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	require.Equal(t, `{"error":"bad request","detail":"helpful details"}`+"\n", recorder.Body.String())
}

func TestNotFound(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/foo", nil)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httputil.NotFound(r.Context(), w, "sorry")
	})
	recorder := httptest.NewRecorder()
	handler(recorder, req)
	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	require.Equal(t, `{"error":"sorry"}`+"\n", recorder.Body.String())
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
	require.Equal(t, `{"error":"internal server error"}`+"\n", recorder.Body.String())
}
