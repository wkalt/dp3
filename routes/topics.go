package routes

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
	"github.com/wkalt/dp3/util/log"
)

func newTopicsHandler(
	tmgr *treemgr.TreeManager,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		database := mux.Vars(r)["database"]
		log.Infow(
			ctx,
			"topics request",
			"database", database,
		)
		tables, err := tmgr.Topics(ctx, database)
		if err != nil {
			httputil.InternalServerError(ctx, w, "failed to get topics: %s", err)
			return
		}
		if err := json.NewEncoder(w).Encode(tables); err != nil {
			httputil.InternalServerError(ctx, w, "failed to encode response: %s", err)
			return
		}
	}
}
