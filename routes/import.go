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

		f, err := os.Open(req.Path) // todo - get from storage provider
		if err != nil {
			httputil.BadRequest(ctx, w, "error opening file: %s", err)
			return
		}
		defer f.Close()
		log.Infof(ctx, "Importing file %s for producer %s", req.Path, req.ProducerID)
		if err := tmgr.Receive(ctx, req.ProducerID, f); err != nil {
			httputil.BadRequest(ctx, w, "error receiving file: %s", err)
			return
		}

		// todo: WAL syncing should be done in a background thread to enable
		// bigger writes to final storage, as well as to decouple write size
		// from input size. For now we just do it synchronously though.
		if err := tmgr.SyncWAL(ctx); err != nil {
			httputil.InternalServerError(ctx, w, "error syncing WAL: %s", err)
			return
		}
		log.Infow(ctx, "Imported", "location", req.Path, "producer_id", req.ProducerID)
	}
}
