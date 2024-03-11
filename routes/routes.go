package routes

import (
	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/http/mw"
	"github.com/wkalt/dp3/treemgr"
)

func MakeRoutes(tmgr *treemgr.TreeManager) *mux.Router {
	r := mux.NewRouter()
	r.Use(mw.WithRequestID)
	r.HandleFunc("/import", newImportHandler(tmgr)).Methods("POST")
	r.HandleFunc("/export", newExportHandler(tmgr)).Methods("POST")
	r.HandleFunc("/statrange", newStatRangeHandler(tmgr)).Methods("POST")
	return r
}
