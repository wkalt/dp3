package routes

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/server/treemgr"
	"github.com/wkalt/dp3/server/util/httputil"
	"github.com/wkalt/dp3/server/util/log"
)

type TablesRequest struct {
	Producer   string `json:"producer"`
	Topic      string `json:"topic"`
	Historical bool   `json:"historical"`
}

func parseRequest(req *http.Request) (TablesRequest, error) {
	query := req.URL.Query()
	producer, err := url.QueryUnescape(query.Get("producer"))
	if err != nil {
		return TablesRequest{}, fmt.Errorf("failed to parse producer: %w", err)
	}
	topic, err := url.QueryUnescape(query.Get("topic"))
	if err != nil {
		return TablesRequest{}, fmt.Errorf("failed to parse topic: %w", err)
	}
	return TablesRequest{
		Producer:   producer,
		Topic:      topic,
		Historical: query.Get("historical") == "true",
	}, nil
}

func newTablesHandler(
	tmgr *treemgr.TreeManager,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req, err := parseRequest(r)
		if err != nil {
			httputil.BadRequest(ctx, w, "failed to parse request: %s", err)
			return
		}
		database := mux.Vars(r)["database"]
		log.Infow(
			ctx,
			"tables request",
			"database", database,
			"producer", req.Producer,
			"topic", req.Topic,
			"historical", req.Historical,
		)
		tables, err := tmgr.GetTables(ctx, database, req.Producer, req.Topic, req.Historical)
		if err != nil {
			httputil.InternalServerError(ctx, w, "failed to get tables: %s", err)
			return
		}
		if err := json.NewEncoder(w).Encode(tables); err != nil {
			httputil.InternalServerError(ctx, w, "failed to encode response: %s", err)
			return
		}
	}
}
