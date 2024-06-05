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

type DeleteRequest struct {
	Database string `json:"database"`
	Producer string `json:"producer"`
	Topic    string `json:"topic"`
	Start    uint64 `json:"start"`
	End      uint64 `json:"end"`
}

func (req DeleteRequest) validate() error {
	if req.Database == "" {
		return errors.New("missing database")
	}
	if req.Producer == "" {
		return errors.New("missing producer")
	}
	if req.Topic == "" {
		return errors.New("missing topic")
	}
	if req.End <= req.Start {
		return errors.New("end must be greater than start")
	}
	return nil
}

func newDeleteHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := DeleteRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %s", err)
			return
		}
		defer r.Body.Close()
		log.Infow(ctx, "delete request",
			"database", req.Database,
			"producer", req.Producer,
			"topic", req.Topic,
			"start", req.Start,
			"end", req.End,
		)
		if err := req.validate(); err != nil {
			httputil.BadRequest(ctx, w, "invalid request: %s", err)
			return
		}
		ctx = log.AddTags(ctx, "producer", req.Producer, "topic", req.Topic)
		if err := tmgr.DeleteMessages(ctx, req.Database, req.Producer, req.Topic, req.Start, req.End); err != nil {
			if errors.Is(err, rootmap.TableNotFoundError{}) {
				httputil.NotFound(ctx, w, "topic %s not found", req.Topic)
				return
			}
			httputil.InternalServerError(ctx, w, "error deleting: %s", err)
			return
		}
	}
}
