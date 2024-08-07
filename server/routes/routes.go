package routes

import (
	"errors"
	"net/http"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/server/treemgr"
	"github.com/wkalt/dp3/server/util/mw"
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
func MakeRoutes(
	tmgr *treemgr.TreeManager,
	allowedOrigins []string,
	sharedKey string,
) *mux.Router {
	r := mux.NewRouter()
	r.Use(
		mw.WithRequestID,
		mw.WithCORSAllowedOrigins(allowedOrigins),
	)
	r.HandleFunc("/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("dp3"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	r.HandleFunc("/export", newExportHandler(tmgr)).Methods("POST")
	r.HandleFunc("/statrange", newStatRangeHandler(tmgr)).Methods("POST", "GET")
	r.HandleFunc("/databases", newDatabasesHandler(tmgr)).Methods("GET")

	r.HandleFunc("/databases/{database}/query", newQueryHandler(tmgr)).Methods("POST")
	r.HandleFunc("/databases/{database}/summarize-children",
		summarizeChildrenHandler(tmgr)).Methods("GET")
	r.HandleFunc("/databases/{database}/topics", newTopicsHandler(tmgr)).Methods("GET")
	r.HandleFunc("/databases/{database}/tables", newTablesHandler(tmgr)).Methods("GET")
	r.HandleFunc("/databases/{database}/producers", newProducersHandler(tmgr)).Methods("GET")
	r.HandleFunc("/databases/{database}/schemas/{hash}", newSchemasHandler(tmgr)).Methods("GET")

	// These use authentication if provided.
	authmw := mw.WithSharedKeyAuth(sharedKey)
	r.Handle("/delete",
		authmw(newDeleteHandler(tmgr))).Methods("POST", "GET")
	r.Handle("/databases/{database}/import",
		authmw(newImportHandler(tmgr))).Methods("POST")
	r.Handle("/databases/{database}/producers/{producer}/import",
		authmw(newImportStreamHandler(tmgr))).Methods("POST", "OPTIONS")

	return r
}
