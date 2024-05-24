package routes

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
	"github.com/wkalt/dp3/util/log"
)

type ImportStreamRequest struct {
	Database string
	Producer string
}

func parseImportStreamRequest(r *http.Request) ImportStreamRequest {
	req := ImportStreamRequest{}
	req.Database = mux.Vars(r)["database"]
	req.Producer = mux.Vars(r)["producer"]
	return req
}

func newImportStreamHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		defer r.Body.Close()
		req := parseImportStreamRequest(r)
		ctx = log.AddTags(ctx, "database", req.Database, "producer", req.Producer)
		log.Infof(ctx, "import stream")
		if err := tmgr.Receive(ctx, req.Database, req.Producer, r.Body); err != nil {
			httputil.InternalServerError(ctx, w, "error receiving file: %w", err)
			return
		}
	}
}
