package routes

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/wkalt/dp3/treemgr"
	"golang.org/x/exp/slog"
)

type IngestRequest struct {
	HashID string `json:"hashID"`
	Path   string `json:"path"`
}

func newIngestHandler(tmgr treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := IngestRequest{}
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

		if err := tmgr.IngestStream(ctx, req.HashID, f); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		slog.InfoContext(ctx, "ingested", "location", req.Path, "hashid", req.HashID)
	}
}
