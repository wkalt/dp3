package storage

import (
	"context"
	"errors"
)

var ErrObjectNotFound = errors.New("object not found")

type Provider interface {
	Put(ctx context.Context, id string, data []byte) error
	GetRange(ctx context.Context, id string, offset int, length int) ([]byte, error)
	Delete(ctx context.Context, id string) error
}
