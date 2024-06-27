package routes

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wkalt/dp3/server/rootmap"
	"github.com/wkalt/dp3/server/treemgr"
	"github.com/wkalt/dp3/server/util/httputil"
	"github.com/wkalt/dp3/server/util/log"
)

// StatRangeRequest is the request body for the statrange endpoint.
type StatRangeRequest struct {
	Database    string `json:"database"`
	Producer    string `json:"producer"`
	Start       uint64 `json:"start"`
	End         uint64 `json:"end"`
	Topic       string `json:"topic"`
	Granularity uint64 `json:"granularity"`
}

func (req StatRangeRequest) validate() error {
	if req.Database == "" {
		return errors.New("missing database")
	}
	if req.Producer == "" {
		return errors.New("missing producer")
	}
	if req.Topic == "" {
		return errors.New("missing topic")
	}
	return nil
}

func newStatRangeHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := StatRangeRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %s", err)
			return
		}
		log.Infow(ctx, "statrange request",
			"database", req.Database,
			"producer", req.Producer,
			"start", req.Start,
			"end", req.End,
			"topic", req.Topic,
			"granularity", req.Granularity,
		)
		if err := req.validate(); err != nil {
			httputil.BadRequest(ctx, w, "invalid request: %s", err)
			return
		}
		summary, err := tmgr.GetStatisticsLatest(
			ctx,
			req.Database,
			req.Producer,
			req.Topic,
			req.Start,
			req.End,
			req.Granularity,
		)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			if errors.Is(err, rootmap.TableNotFoundError{}) {
				httputil.NotFound(ctx, w, "no matching data")
				return
			}
			httputil.InternalServerError(ctx, w, "error getting statistics: %s", err)
			return
		}
		if err := json.NewEncoder(w).Encode(summary); err != nil {
			httputil.InternalServerError(ctx, w, "error encoding response: %s", err)
		}
	}
}
