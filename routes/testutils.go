package routes

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/wkalt/dp3/treemgr"
)

func MakeTestRoutes(ctx context.Context, t *testing.T, tmgr *treemgr.TreeManager) (string, func()) {
	t.Helper()
	handler := MakeRoutes(tmgr, nil)
	srv := httptest.NewServer(handler)
	return srv.URL, srv.Close
}
