package routes

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wkalt/dp3/server/treemgr"
	"github.com/wkalt/dp3/server/util/httputil"
	"github.com/wkalt/dp3/server/util/log"
)

// ExportRequest is the request body for the export endpoint.
type ExportRequest struct {
	Database string            `json:"database"`
	Producer string            `json:"producer"`
	Topics   map[string]uint64 `json:"topics"`
	Start    uint64            `json:"start"`
	End      uint64            `json:"end"`
}

func (req ExportRequest) validate() error {
	if req.Database == "" {
		return errors.New("missing database")
	}
	if req.Producer == "" {
		return errors.New("missing producer")
	}
	return nil
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
			"producer", req.Producer,
			"topics", req.Topics,
			"start", req.Start,
			"end", req.End,
		)
		if err := req.validate(); err != nil {
			httputil.BadRequest(ctx, w, "invalid request: %s", err)
			return
		}
		// negotiate request -> versioned roots
		roots, err := tmgr.GetLatestRoots(ctx, req.Database, req.Producer, req.Topics)
		if err != nil {
			httputil.InternalServerError(ctx, w, "error getting latest roots: %s", err)
			return
		}

		// send back an argument in the headers that the client can use to poll
		header := make(map[string]uint64)
		for _, root := range roots {
			header[root.Topic] = root.Version
		}
		headerData, err := json.Marshal(header)
		if err != nil {
			httputil.InternalServerError(ctx, w, "error encoding header: %s", err)
			return
		}
		w.Header().Add("X-Topics", string(headerData))

		if len(roots) == 0 {
			w.WriteHeader(http.StatusAccepted) // todo: status code abuse
		}

		if err := tmgr.GetMessages(ctx, w, req.Start, req.End, roots); err != nil {
			if err := clientError(err); err != nil {
				log.Infof(ctx, "Client closed connection: %s", err)
				return
			}
			httputil.InternalServerError(ctx, w, "error getting messages: %s", err)
		}
	}
}
