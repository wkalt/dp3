package mw

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/wkalt/dp3/util/log"
)

func WithRequestID(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := uuid.New()
		ctx = log.AddTags(ctx, "request_id", id.String())
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}
