package routes

import (
	"encoding/json"
	"net/http"

	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
	"github.com/wkalt/dp3/util/log"
)

type DeleteRequest struct {
	Database   string `json:"database"`
	ProducerID string `json:"producerId"`
	Topic      string `json:"topic"`
	Start      uint64 `json:"start"`
	End        uint64 `json:"end"`
}

func newDeleteHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := DeleteRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %s", err)
			return
		}
		defer r.Body.Close()
		ctx = log.AddTags(ctx, "producer", req.ProducerID, "topic", req.Topic)
		if err := tmgr.DeleteMessages(ctx, req.Database, req.ProducerID, req.Topic, req.Start, req.End); err != nil {
			httputil.InternalServerError(ctx, w, "error deleting: %s", err)
			return
		}
	}
}
