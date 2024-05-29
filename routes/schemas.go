package routes

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/wkalt/dp3/schemastore"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util/httputil"
)

func newSchemasHandler(tmgr *treemgr.TreeManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		database := mux.Vars(r)["database"]
		hash := mux.Vars(r)["hash"]
		schema, err := tmgr.GetSchema(ctx, database, hash)
		if err != nil {
			if errors.Is(err, schemastore.ErrSchemaNotFound) {
				httputil.NotFound(ctx, w, "schema not found")
				return
			}
			httputil.InternalServerError(ctx, w, err.Error())
			return
		}
		bytes, err := json.Marshal(map[string]any{
			"name":     schema.Name,
			"encoding": schema.Encoding,
			"data":     schema.Data,
		})
		if err != nil {
			httputil.InternalServerError(ctx, w, err.Error())
			return
		}
		if _, err = w.Write(bytes); err != nil {
			httputil.InternalServerError(ctx, w, err.Error())
			return
		}
	}
}
