package routes

import (
	"errors"
	"net/http"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/mw"
)

/*
The routes module contains the HTTP routes for the DP3 service. These are
currently very loose and uncommitted APIs, that just exist to demonstrate the
functionality of the database. Eventually we will formalize the design and
decide whether to stick with REST or switch to gRPC.
*/

////////////////////////////////////////////////////////////////////////////////

func clientError(err error) error {
	switch {
	case errors.Is(err, syscall.ECONNRESET):
		return syscall.ECONNRESET
	case errors.Is(err, syscall.EPIPE):
		return syscall.EPIPE
	}
	return nil
}

// MakeRoutes creates a new router with all the routes for the DP3 service.
func MakeRoutes(tmgr *treemgr.TreeManager) *mux.Router {
	r := mux.NewRouter()
	r.Use(
		mw.WithRequestID,
		mw.WithCORSAllowedOrigins([]string{"http://localhost:5173"}),
	)
	r.HandleFunc("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("DP3"))
	}))
	r.HandleFunc("/import", newImportHandler(tmgr)).Methods("POST")
	r.HandleFunc("/export", newExportHandler(tmgr)).Methods("POST")
	r.HandleFunc("/query", newQueryHandler(tmgr)).Methods("POST")
	r.HandleFunc("/statrange", newStatRangeHandler(tmgr)).Methods("POST", "GET")
	r.HandleFunc("/delete", newDeleteHandler(tmgr)).Methods("POST", "GET")
	r.HandleFunc("/truncate", newTruncateHandler(tmgr)).Methods("POST", "GET")
	r.HandleFunc("/databases", newDatabasesHandler(tmgr)).Methods("GET")

	r.HandleFunc("/databases/{database}/topics", newTopicsHandler(tmgr)).Methods("GET")
	r.HandleFunc("/databases/{database}/tables", newTablesHandler(tmgr)).Methods("GET")
	r.HandleFunc("/databases/{database}/producers", newProducersHandler(tmgr)).Methods("GET")
	return r
}
