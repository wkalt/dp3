package storage

import (
	"errors"
)

var ErrObjectNotFound = errors.New("object not found")

type Provider interface {
	Put(uint64, []byte) error
	Get(uint64) ([]byte, error)
	Delete(uint64) error
}
