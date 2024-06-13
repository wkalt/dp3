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
		mw.WithCORSAllowedOrigins([]string{
			"http://localhost:5174",
			"http://localhost:5173",
			"http://localhost:8080",
		}),
	)
	r.HandleFunc("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("dp3"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	r.HandleFunc("/export", newExportHandler(tmgr)).Methods("POST")
	r.HandleFunc("/statrange", newStatRangeHandler(tmgr)).Methods("POST", "GET")
	r.HandleFunc("/delete", newDeleteHandler(tmgr)).Methods("POST", "GET")
	r.HandleFunc("/databases", newDatabasesHandler(tmgr)).Methods("GET")

	r.HandleFunc("/databases/{database}/import", newImportHandler(tmgr)).Methods("POST")
	r.HandleFunc("/databases/{database}/query", newQueryHandler(tmgr)).Methods("POST")
	r.HandleFunc("/databases/{database}/summarize-children",
		summarizeChildrenHandler(tmgr)).Methods("GET")
	r.HandleFunc("/databases/{database}/producers/{producer}/import",
		newImportStreamHandler(tmgr)).Methods("POST", "OPTIONS")
	r.HandleFunc("/databases/{database}/topics", newTopicsHandler(tmgr)).Methods("GET")
	r.HandleFunc("/databases/{database}/tables", newTablesHandler(tmgr)).Methods("GET")
	r.HandleFunc("/databases/{database}/producers", newProducersHandler(tmgr)).Methods("GET")
	r.HandleFunc("/databases/{database}/schemas/{hash}", newSchemasHandler(tmgr)).Methods("GET")
	return r
}
