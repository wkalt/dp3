package routes

import (
	"encoding/json"
	"net/http"

	"github.com/wkalt/dp3/treemgr"
)

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
			http.Error(w, err.Error(), http.StatusBadRequest)
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(summary); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
