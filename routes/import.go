package routes

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/wkalt/dp3/treemgr"
	"golang.org/x/exp/slog"
)

type ImportRequest struct {
	ProducerID string `json:"producerId"`
	Path       string `json:"path"`
}

func newImportHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := ImportRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		f, err := os.Open(req.Path) // todo - get from storage provider
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer f.Close()

		if err := tmgr.Receive(ctx, req.ProducerID, f); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		slog.InfoContext(ctx, "imported", "location", req.Path, "producerID", req.ProducerID)
	}
}
