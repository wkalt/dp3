package routes

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
	"github.com/wkalt/dp3/util/log"
)

// ImportRequest is the request body for the import endpoint.
type ImportRequest struct {
	Database   string `json:"database"`
	ProducerID string `json:"producerId"`
	Path       string `json:"path"`
}

func newImportHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := ImportRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %s", err)
			return
		}
		defer r.Body.Close()
		ctx = log.AddTags(ctx, "database", req.Database, "producer", req.ProducerID, "path", req.Path)
		f, err := os.Open(req.Path) // todo - get from storage provider
		if err != nil {
			httputil.BadRequest(ctx, w, "error opening file: %s", err)
			return
		}
		defer f.Close()
		log.Infof(ctx, "Importing file")
		if err := tmgr.Receive(ctx, req.Database, req.ProducerID, f); err != nil {
			httputil.InternalServerError(ctx, w, "error receiving file: %s", err)
			return
		}
		log.Infof(ctx, "Imported")
	}
}
