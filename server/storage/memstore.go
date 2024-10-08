package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/wkalt/dp3/server/util"
)

/*
Memstore is an in-memory storage provider backed by a map. It is only suitable for tests.
*/

////////////////////////////////////////////////////////////////////////////////

// MemStore is an in-memory store.
type MemStore struct {
	data map[string][]byte
	mtx  *sync.RWMutex
}

func (m *MemStore) Get(_ context.Context, id string) (io.ReadCloser, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	data, ok := m.data[id]
	if !ok {
		return nil, ErrObjectNotFound
	}

	return util.NewReadSeekNopCloser(bytes.NewReader(data)), nil
}

// Put stores an object in the store.
func (m *MemStore) Put(_ context.Context, id string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("failed to read input: %w", err)
	}
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.data[id] = data
	return nil
}

// Get retrieves an object from the store.
func (m *MemStore) GetRange(_ context.Context, id string, offset int, length int) (io.ReadSeekCloser, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	data, ok := m.data[id]
	if !ok {
		return nil, ErrObjectNotFound
	}

	return util.NewReadSeekNopCloser(bytes.NewReader(data[offset : offset+length])), nil
}

// Delete removes an object from the store.
func (m *MemStore) Delete(_ context.Context, id string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	delete(m.data, id)
	return nil
}

func (m *MemStore) String() string {
	return "memory"
}

// NewMemStore returns a new in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		data: make(map[string][]byte),
		mtx:  &sync.RWMutex{},
	}
}
