package storage

import "errors"

var ErrNotFound = errors.New("not found")

type Provider interface {
	Put(int64, []byte) error
	Get(int64) ([]byte, error)
}
