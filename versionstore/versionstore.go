package versionstore

import "context"

type Versionstore interface {
	Next(context.Context) (uint64, error)
}

type memVersionStore struct {
	next uint64
}

func (m *memVersionStore) Next(ctx context.Context) (uint64, error) {
	m.next++
	return m.next, nil
}

func NewMemVersionStore() Versionstore {
	return &memVersionStore{
		next: 1,
	}
}
