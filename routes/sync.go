package routes

import (
	"net/http"

	"github.com/wkalt/dp3/treemgr"
)

func newSyncHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if err := tmgr.SyncWAL(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}
