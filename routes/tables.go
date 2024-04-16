package routes

import (
	"encoding/json"
	"net/http"

	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
)

type TablesRequest struct {
	Producer   string `json:"producer"`
	Topic      string `json:"topic"`
	Historical bool   `json:"historical"`
}

func newTablesHandler(
	tmgr *treemgr.TreeManager,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := TablesRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "failed to decode request: %s", err)
			return
		}
		tables, err := tmgr.GetTables(ctx, req.Producer, req.Topic, req.Historical)
		if err != nil {
			httputil.InternalServerError(ctx, w, "failed to get tables: %s", err)
			return
		}
		if err := json.NewEncoder(w).Encode(tables); err != nil {
			httputil.InternalServerError(ctx, w, "failed to encode response: %s", err)
			return
		}
	}
}
