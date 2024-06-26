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

// WithCORSAllowedOrigins is a middleware that allows requests from specified
// origins.
func WithCORSAllowedOrigins(origins []string) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			for _, o := range origins {
				if o == origin {
					w.Header().Set("Access-Control-Allow-Origin", o)
					w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
					break
				}
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			h.ServeHTTP(w, r)
		})
	}
}
