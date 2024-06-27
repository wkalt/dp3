package schemastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/storage"
	"github.com/wkalt/dp3/server/util"
)

var ErrSchemaNotFound = errors.New("schema not found")

type key struct {
	database string
	hash     string
}

type SchemaStore struct {
	cache  *util.LRU[key, *mcap.Schema]
	store  storage.Provider
	prefix string

	mtx *sync.RWMutex
}

type internalSchema struct {
	Name     string `json:"name"`
	Encoding string `json:"encoding"`
	Data     []byte `json:"data"`
}

func toSchema(s *mcap.Schema) *internalSchema {
	return &internalSchema{
		Name:     s.Name,
		Encoding: s.Encoding,
		Data:     s.Data,
	}
}

func fromSchema(s internalSchema) *mcap.Schema {
	return &mcap.Schema{
		ID:       0, // NB: invalid ID for mcap writing.
		Name:     s.Name,
		Encoding: s.Encoding,
		Data:     s.Data,
	}
}

// NewSchemaStore creates a new SchemaStore.
func NewSchemaStore(store storage.Provider, prefix string, capacity uint64) *SchemaStore {
	return &SchemaStore{
		cache:  util.NewLRU[key, *mcap.Schema](capacity),
		store:  store,
		prefix: prefix,
		mtx:    &sync.RWMutex{},
	}
}

// Put stores a schema in the store.
func (s *SchemaStore) Put(
	ctx context.Context,
	database string,
	hash string,
	schema *mcap.Schema,
) error {
	// serialize puts across both the (network) object upload and cache update
	// to avoid a bunch of duplicate submissions on initial observation of new
	// data streams. A map of key-level mutexes could be more appropriate to
	// reduce contention across topics but that would require a key-level LRU
	// cache as well. If contention here becomes an issue we can revisit.
	// Usually schemas are very conserved within an organization or "database".
	s.mtx.Lock()
	defer s.mtx.Unlock()

	converted := toSchema(schema)
	data, err := json.Marshal(converted)
	if err != nil {
		return fmt.Errorf("failed to serialize schema: %w", err)
	}
	id := path.Join(database, s.prefix, hash)
	if err := s.store.Put(ctx, id, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("failed to put schema to storage: %w", err)
	}
	s.cacheSchema(database, hash, schema)
	return nil
}

func (s *SchemaStore) cacheSchema(
	database string,
	hash string,
	schema *mcap.Schema,
) {
	s.cache.Put(
		key{database, hash},
		schema,
		uint64(len(schema.Data)+len(schema.Name)+len(schema.Encoding)),
	)
}

func (s *SchemaStore) checkCache(database, hash string) (*mcap.Schema, bool) {
	schema, ok := s.cache.Get(key{database, hash})
	return schema, ok
}

func (s *SchemaStore) Get(
	ctx context.Context,
	database string,
	hash string,
) (*mcap.Schema, error) {
	schema, ok := s.checkCache(database, hash)
	if ok {
		return schema, nil
	}
	id := path.Join(database, s.prefix, hash)
	reader, err := s.store.Get(ctx, id)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, ErrSchemaNotFound
		}
		return nil, fmt.Errorf("failed to find schema: %w", err)
	}
	defer reader.Close()
	bytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}
	internal := internalSchema{}
	if err := json.Unmarshal(bytes, &internal); err != nil {
		return nil, fmt.Errorf("failed to deserialize schema data: %w", err)
	}
	// cache on read
	schema = fromSchema(internal)
	s.cacheSchema(database, hash, schema)
	return schema, nil
}
