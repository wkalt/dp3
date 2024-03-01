package storage

import (
	"errors"
)

var ErrObjectNotFound = errors.New("object not found")

type Provider interface {
	Put(id string, data []byte) error
	GetRange(id string, offset int, length int) ([]byte, error)
	Delete(id string) error
}
