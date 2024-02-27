package storage

import (
	"errors"
)

var ErrNotFound = errors.New("not found")

type Provider interface {
	Put(uint64, []byte) error
	Get(uint64) ([]byte, error)
	Delete(uint64) error
}
