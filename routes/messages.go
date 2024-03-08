package routes

import (
	"encoding/json"
	"net/http"

	"github.com/wkalt/dp3/treemgr"
	"golang.org/x/exp/slog"
)

type MessagesRequest struct {
	ProducerID string   `json:"producerId"`
	Topics     []string `json:"topics"`
	Start      uint64   `json:"start"`
	End        uint64   `json:"end"`
}

func newMessagesHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := MessagesRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			slog.ErrorContext(ctx, "error decoding request", "error", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		slog.InfoContext(ctx, "messages request",
			"hashid", req.ProducerID,
			"topics", req.Topics,
			"start", req.Start,
			"end", req.End,
		)
		err := tmgr.GetMessagesLatest(ctx, w, req.Start*1e9, req.End*1e9, req.ProducerID, req.Topics)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}
