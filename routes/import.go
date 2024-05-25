package routes

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
	"github.com/wkalt/dp3/util/log"
)

// ImportRequest is the request body for the import endpoint.
type ImportRequest struct {
	ProducerID string `json:"producerId"`
	Path       string `json:"path"`
}

func (req ImportRequest) validate() error {
	if req.ProducerID == "" {
		return errors.New("missing producerId")
	}
	if req.Path == "" {
		return errors.New("missing path")
	}
	return nil
}

func newImportHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := ImportRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %w", err)
			return
		}
		defer r.Body.Close()
		database := mux.Vars(r)["database"]
		ctx = log.AddTags(ctx, "database", database, "producer", req.ProducerID, "path", req.Path)
		if err := req.validate(); err != nil {
			httputil.BadRequest(ctx, w, "invalid request: %w", err)
			return
		}
		f, err := os.Open(req.Path) // todo - get from storage provider
		if err != nil {
			httputil.BadRequest(ctx, w, "error opening file: %w", err)
			return
		}
		defer f.Close()
		log.Infof(ctx, "Importing file")
		if err := tmgr.Receive(ctx, database, req.ProducerID, f); err != nil {
			httputil.InternalServerError(ctx, w, "error receiving file: %w", err)
			return
		}
		log.Infof(ctx, "Imported")
	}
}
