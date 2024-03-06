package routes

import (
	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/treemgr"
)

func MakeRoutes(tmgr treemgr.TreeManager) *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/ingest", newIngestHandler(tmgr)).Methods("POST")
	r.HandleFunc("/sync", newSyncHandler(tmgr)).Methods("POST")
	return r
}
