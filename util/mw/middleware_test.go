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
	"github.com/wkalt/dp3/util/log"
	"github.com/wkalt/dp3/util/mw"
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
