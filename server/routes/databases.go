package routes

import (
	"encoding/json"
	"net/http"

	"github.com/wkalt/dp3/server/treemgr"
	"github.com/wkalt/dp3/server/util/httputil"
)

func newDatabasesHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		databases, err := tmgr.Databases(ctx)
		if err != nil {
			httputil.InternalServerError(ctx, w, "failed to get databases: %s", err)
			return
		}
		if err := json.NewEncoder(w).Encode(databases); err != nil {
			httputil.InternalServerError(ctx, w, "failed to encode response: %s", err)
			return
		}
	}
}
