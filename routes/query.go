package routes

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wkalt/dp3/executor"
	"github.com/wkalt/dp3/plan"
	"github.com/wkalt/dp3/ql"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
	"github.com/wkalt/dp3/util/log"
)

/*
The query route receives query strings in the dp3 query language, compiles them
into an execution tree, and executes the query.
*/

////////////////////////////////////////////////////////////////////////////////

// QueryRequest represents a query request.
type QueryRequest struct {
	Database string `json:"database"`
	Query    string `json:"query"`
}

func (req QueryRequest) validate() error {
	if req.Database == "" {
		return errors.New("missing database")
	}
	if req.Query == "" {
		return errors.New("missing query")
	}
	return nil
}

// newQueryHandler creates a new query handler.
func newQueryHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	parser := ql.NewParser()
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req := QueryRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.BadRequest(ctx, w, "error decoding request: %s", err)
			return
		}
		log.Infow(ctx, "query request",
			"database", req.Database,
			"query", req.Query,
		)
		if err := req.validate(); err != nil {
			httputil.BadRequest(ctx, w, "invalid request: %s", err)
			return
		}
		ast, err := parser.ParseString("", req.Query)
		if err != nil {
			httputil.BadRequest(ctx, w, "error parsing query: %s", err)
			return
		}
		qp, err := plan.CompileQuery(req.Database, *ast)
		if err != nil {
			httputil.InternalServerError(ctx, w, "error compiling query: %s", err)
			return
		}
		log.Debugf(ctx, "compiled query: %s", qp.String())
		if err := executor.Run(ctx, w, qp, tmgr.NewTreeIterator); err != nil {
			fieldNotFound := executor.FieldNotFoundError{}
			if errors.As(err, &fieldNotFound) {
				httputil.BadRequest(ctx, w, "%s", fieldNotFound.Error())
				return
			}
			streamNotFound := rootmap.StreamNotFoundError{}
			if errors.As(err, &streamNotFound) {
				httputil.BadRequest(ctx, w, "%s", streamNotFound.Error())
				return
			}
			httputil.InternalServerError(ctx, w, "error executing query: %s", err)
			return
		}
	}
}
