package routes

import (
	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/http/mw"
	"github.com/wkalt/dp3/treemgr"
)

/*
The routes module contains the HTTP routes for the DP3 service. These are
currently very loose and uncommitted APIs, that just exist to demonstrate the
functionality of the database. Eventually we will formalize the design and
decide whether to stick with REST or switch to gRPC.
*/

////////////////////////////////////////////////////////////////////////////////

// MakeRoutes creates a new router with all the routes for the DP3 service.
func MakeRoutes(tmgr *treemgr.TreeManager) *mux.Router {
	r := mux.NewRouter()
	r.Use(mw.WithRequestID)
	r.HandleFunc("/import", newImportHandler(tmgr)).Methods("POST")
	r.HandleFunc("/export", newExportHandler(tmgr)).Methods("POST")
	r.HandleFunc("/statrange", newStatRangeHandler(tmgr)).Methods("POST")
	return r
}
