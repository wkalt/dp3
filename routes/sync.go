package routes

import (
	"net/http"

	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
)

func newSyncHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if err := tmgr.SyncWAL(ctx); err != nil {
			httputil.InternalServerError(ctx, w, "error syncing WAL: %s", err)
			return
		}
	}
}
