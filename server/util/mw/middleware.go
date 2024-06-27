package mw

import (
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/wkalt/dp3/server/util/httputil"
	"github.com/wkalt/dp3/server/util/log"
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

func parseBearerToken(authHeader string) string {
	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || parts[0] != "Bearer" {
		return ""
	}
	return parts[1]
}

// WithSharedKeyAuth is a middleware that requires a shared key to be present in
// the Authorization header. This is only suitable for demo purposes and must be
// replaced with either a more robust mechanism, or a more intelligent auth
// proxy external to the service.
func WithSharedKeyAuth(key string) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if key != "" {
				ctx := r.Context()
				token := parseBearerToken(r.Header.Get("Authorization"))
				if token != key {
					httputil.Unauthorized(ctx, w, "invalid token")
					return
				}
			}
			h.ServeHTTP(w, r)
		})
	}
}
