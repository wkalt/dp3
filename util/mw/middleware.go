package mw

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/wkalt/dp3/util/log"
)

/*
mw contains http middlewares.
*/

////////////////////////////////////////////////////////////////////////////////

// WithRequestID is a middleware that adds a request ID to the context of each
// request.
func WithRequestID(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := uuid.New()
		ctx = log.AddTags(ctx, "request_id", id.String())
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}
