package routes

import (
	"encoding/json"
	"net/http"

	"github.com/wkalt/dp3/query/executor"
	"github.com/wkalt/dp3/query/plan"
	"github.com/wkalt/dp3/query/ql"
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
	Query string `json:"query"`
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
			"query", req.Query,
		)
		ast, err := parser.ParseString("", req.Query)
		if err != nil {
			httputil.BadRequest(ctx, w, "error parsing query: %s", err)
			return
		}
		qp, err := plan.CompileQuery(*ast)
		if err != nil {
			httputil.InternalServerError(ctx, w, "error compiling query: %s", err)
			return
		}
		if err := executor.Run(ctx, w, qp, tmgr.NewTreeIterator); err != nil {
			httputil.InternalServerError(ctx, w, "error executing query: %s", err)
			return
		}
	}
}
