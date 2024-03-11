package routes

import (
	"encoding/json"
	"net/http"

	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
	"github.com/wkalt/dp3/util/log"
)

type ExportRequest struct {
	ProducerID string   `json:"producerId"`
	Topics     []string `json:"topics"`
	Start      uint64   `json:"start"`
	End        uint64   `json:"end"`
}

func newExportHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := ExportRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %s", err)
			return
		}
		log.Infow(ctx, "export request",
			"producer_id", req.ProducerID,
			"topics", req.Topics,
			"start", req.Start,
			"end", req.End,
		)
		if err := tmgr.GetMessagesLatest(ctx, w, req.Start, req.End, req.ProducerID, req.Topics); err != nil {
			httputil.InternalServerError(ctx, w, "error getting messages: %s", err)
			return
		}
	}
}
