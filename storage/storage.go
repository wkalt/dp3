package storage

import (
	"errors"
)

var ErrObjectNotFound = errors.New("object not found")

type Provider interface {
	Put(id uint64, data []byte) error
	Get(id uint64) ([]byte, error)
	Delete(id uint64) error
}
