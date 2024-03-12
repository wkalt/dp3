package routes

import (
	"encoding/json"
	"net/http"

	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
)

// StatRangeRequest is the request body for the statrange endpoint.
type StatRangeRequest struct {
	ProducerID  string `json:"producerId"`
	Start       uint64 `json:"start"`
	End         uint64 `json:"end"`
	Topic       string `json:"topic"`
	Granularity uint64 `json:"granularity"`
}

func newStatRangeHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := StatRangeRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %s", err)
			return
		}
		summary, err := tmgr.GetStatisticsLatest(
			ctx,
			req.Start,
			req.End,
			req.ProducerID,
			req.Topic,
			req.Granularity,
		)
		if err != nil {
			httputil.InternalServerError(ctx, w, "error getting statistics: %s", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(summary); err != nil {
			httputil.InternalServerError(ctx, w, "error encoding response: %s", err)
		}
	}
}
