package httputil

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/wkalt/dp3/util/log"
)

/*
httputil contains utility functions for HTTP error responses. Any error
generated in a handler should go through one of these, to ensure we are logging
and responding to the client in a consistent way.
*/

////////////////////////////////////////////////////////////////////////////////

type ErrorResponse struct {
	Error string `json:"error"`
}

func writeErrorResponse(ctx context.Context, w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	resp := ErrorResponse{Error: msg}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Errorw(ctx, "error writing response", "error", err)
	}
}

func NotFound(ctx context.Context, w http.ResponseWriter, msg string, args ...any) {
	log.Debugw(ctx, "Not found", "msg", fmt.Sprintf(msg, args...))
	writeErrorResponse(ctx, w, http.StatusNotFound, fmt.Sprintf(msg, args...))
}

func BadRequest(ctx context.Context, w http.ResponseWriter, msg string, args ...any) {
	log.Errorw(ctx, "Bad request", "msg", fmt.Sprintf(msg, args...))
	writeErrorResponse(ctx, w, http.StatusBadRequest, fmt.Sprintf(msg, args...))
}

func InternalServerError(ctx context.Context, w http.ResponseWriter, msg string, args ...any) {
	log.Errorw(ctx, "Internal server error", "msg", fmt.Sprintf(msg, args...))
	writeErrorResponse(ctx, w, http.StatusInternalServerError, "Internal server error")
}
