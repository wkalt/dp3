package storage

import (
	"context"
	"errors"
	"io"
)

/*
The storage provider interface describes the minimal set of operations required
by persistent storage. These must be supported by any popular object storage
implementation.
*/

////////////////////////////////////////////////////////////////////////////////

// ErrObjectNotFound is returned when an object is not found.
var ErrObjectNotFound = errors.New("object not found")

// Provider is the interface for a storage provider.
type Provider interface {
	Put(ctx context.Context, id string, r io.Reader) error
	Get(ctx context.Context, id string) (io.ReadCloser, error)
	GetRange(ctx context.Context, id string, offset int, length int) (io.ReadSeekCloser, error)
	Delete(ctx context.Context, id string) error
}
