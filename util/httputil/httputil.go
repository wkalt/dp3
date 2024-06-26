package httputil

import (
	"context"
	"encoding/json"
	"errors"
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

// Detailer is an interface for errors that can provide a detailed message.
type Detailer interface {
	Detail() string
}

func detail(err error) string {
	for ; err != nil; err = errors.Unwrap(err) {
		if d, ok := err.(Detailer); ok {
			return d.Detail()
		}
	}
	return ""
}

// ErrorResponse is the structure of an error response. The Detail field is
// optional and will be omitted from the JSON serialization if unsupplied.
type ErrorResponse struct {
	Error  string `json:"error"`
	Detail string `json:"detail,omitempty"`
}

func writeErrorResponse(ctx context.Context, w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	resp := ErrorResponse{Error: err.Error()}
	if details := detail(err); details != "" {
		resp.Detail = details
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Errorw(ctx, "error writing response", "error", err)
	}
}

// NotFound logs the error and sends a 404 response to the client.
func NotFound(ctx context.Context, w http.ResponseWriter, msg string, args ...any) {
	log.Debugw(ctx, "Not found", "msg", fmt.Errorf(msg, args...))
	writeErrorResponse(ctx, w, http.StatusNotFound, fmt.Errorf(msg, args...))
}

// BadRequest logs the error and sends a 400 response to the client.
func BadRequest(ctx context.Context, w http.ResponseWriter, msg string, args ...any) {
	log.Errorw(ctx, "Bad request", "msg", fmt.Errorf(msg, args...))
	writeErrorResponse(ctx, w, http.StatusBadRequest, fmt.Errorf(msg, args...))
}

// InternalServerError logs the error and sends a 500 response to the client
// with a generic message.
func InternalServerError(ctx context.Context, w http.ResponseWriter, msg string, args ...any) {
	log.Errorw(ctx, "Internal server error", "msg", fmt.Errorf(msg, args...))
	writeErrorResponse(ctx, w, http.StatusInternalServerError, errors.New("internal server error"))
}

// Unauthorized logs the error and sends a 401 response to the client.
func Unauthorized(ctx context.Context, w http.ResponseWriter, msg string, args ...any) {
	log.Debugw(ctx, "Unauthorized", "msg", fmt.Errorf(msg, args...))
	writeErrorResponse(ctx, w, http.StatusUnauthorized, fmt.Errorf(msg, args...))
}
