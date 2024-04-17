package routes

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
	"github.com/wkalt/dp3/util/log"
)

type TablesRequest struct {
	Database   string `json:"database"`
	Producer   string `json:"producer"`
	Topic      string `json:"topic"`
	Historical bool   `json:"historical"`
}

func (req TablesRequest) validate() error {
	if req.Database == "" {
		return errors.New("missing database")
	}
	return nil
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
		log.Infow(
			ctx,
			"tables request",
			"database", req.Database,
			"producer", req.Producer,
			"topic", req.Topic,
			"historical", req.Historical,
		)
		if err := req.validate(); err != nil {
			httputil.BadRequest(ctx, w, "invalid request: %s", err)
			return
		}
		tables, err := tmgr.GetTables(ctx, req.Database, req.Producer, req.Topic, req.Historical)
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
