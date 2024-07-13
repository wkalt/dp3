package routes

import (
	"net/http/httptest"
	"testing"

	"github.com/wkalt/dp3/server/treemgr"
)

func MakeTestRoutes(t *testing.T, tmgr *treemgr.TreeManager) (string, func()) {
	t.Helper()
	handler := MakeRoutes(tmgr, nil, "")
	srv := httptest.NewServer(handler)
	return srv.URL, srv.Close
}
