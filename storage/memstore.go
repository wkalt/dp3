package storage

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type MemStore struct {
	data map[uint64][]byte
	mtx  *sync.RWMutex
}

func (m *MemStore) Put(id uint64, data []byte) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.data[id] = data
	return nil
}

func (m *MemStore) Get(id uint64) ([]byte, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	data, ok := m.data[id]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return data, nil
}

func (m *MemStore) Delete(id uint64) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	delete(m.data, id)
	return nil
}

func (m *MemStore) String() string {
	ids := make([]uint64, 0, len(m.data))
	for id := range m.data {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	sb := &strings.Builder{}
	sb.WriteString("{")
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%d: \"%s\"", id, string(m.data[id])))
	}
	sb.WriteString("}")
	return sb.String()
}

func NewMemStore() *MemStore {
	return &MemStore{
		data: make(map[uint64][]byte),
		mtx:  &sync.RWMutex{},
	}
}