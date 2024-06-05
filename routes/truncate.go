package routes

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
	"github.com/wkalt/dp3/util/log"
)

// TruncateRequest is the request body for the truncate endpoint.
type TruncateRequest struct {
	Database  string `json:"database"`
	Producer  string `json:"producer"`
	Topic     string `json:"topic"`
	Timestamp int64  `json:"timestamp"`
}

func (req TruncateRequest) validate() error {
	if req.Database == "" {
		return errors.New("missing database")
	}
	if req.Producer == "" {
		return errors.New("missing producer")
	}
	if req.Topic == "" {
		return errors.New("missing topic")
	}
	return nil
}

func newTruncateHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := TruncateRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %s", err)
			return
		}
		defer r.Body.Close()
		log.Infow(ctx, "truncate request",
			"database", req.Database,
			"producer", req.Producer,
			"topic", req.Topic,
			"timestamp", req.Timestamp,
		)
		if err := req.validate(); err != nil {
			httputil.BadRequest(ctx, w, "invalid request: %s", err)
			return
		}
		ctx = log.AddTags(ctx, "producer", req.Producer, "topic", req.Topic, "timestamp", req.Timestamp)
		if err := tmgr.Truncate(ctx, req.Database, req.Producer, req.Topic, req.Timestamp); err != nil {
			if errors.Is(err, rootmap.TableNotFoundError{}) {
				httputil.NotFound(ctx, w, "topic %s not found", req.Topic)
				return
			}
			httputil.InternalServerError(ctx, w, "error truncating: %s", err)
			return
		}
	}
}
