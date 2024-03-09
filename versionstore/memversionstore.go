package versionstore

import (
	"context"
	"sync"
)

type memVersionStore struct {
	next uint64
	mtx  *sync.Mutex
}

func (m *memVersionStore) Next(ctx context.Context) (uint64, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.next++
	return m.next, nil
}

func NewMemVersionStore() Versionstore {
	return &memVersionStore{
		next: 1,
		mtx:  &sync.Mutex{},
	}
}
