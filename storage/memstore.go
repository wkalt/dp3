package storage

import (
	"sync"
)

// MemStore is an in-memory store.
type MemStore struct {
	data map[uint64][]byte
	mtx  *sync.RWMutex
}

// Put stores an object in the store.
func (m *MemStore) Put(id uint64, data []byte) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.data[id] = data
	return nil
}

// Get retrieves an object from the store.
func (m *MemStore) Get(id uint64) ([]byte, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	data, ok := m.data[id]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return data, nil
}

// Delete removes an object from the store.
func (m *MemStore) Delete(id uint64) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	delete(m.data, id)
	return nil
}

// NewMemStore returns a new in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		data: make(map[uint64][]byte),
		mtx:  &sync.RWMutex{},
	}
}
