package nodestore

import (
	"errors"
	"fmt"

	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

type Nodestore struct {
	store      storage.Provider
	cache      *util.LRU[Node]
	nextNodeID uint64
	maxNodeID  uint64
}

func (n *Nodestore) generateNodeID(node Node) uint64 {
	id := n.nextNodeID
	n.nextNodeID++
	if _, ok := node.(*LeafNode); ok {
		return id | 1<<63
	}
	return id & ^(uint64(1) << 63)
}

func isLeafID(id uint64) bool {
	return id>>63 == 1
}

func bytesToNode(id uint64, value []byte) (Node, error) {
	if isLeafID(id) {
		node := NewLeafNode(0, nil)
		if err := node.FromBytes(value); err != nil {
			return nil, fmt.Errorf("failed to parse leaf node: %w", err)
		}
		return node, nil
	}
	node := NewInnerNode(0, 0, 0, 0)
	if err := node.FromBytes(value); err != nil {
		return nil, fmt.Errorf("failed to parse inner node: %w", err)
	}
	return node, nil
}

// Get retrieves a node from the store. If the node is not in the cache, it will be loaded from the store.
func (n *Nodestore) Get(id uint64) (Node, error) {
	if value, ok := n.cache.Get(id); ok {
		return value, nil
	}
	data, err := n.store.Get(id)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, ErrNodeNotFound
		}
		return nil, fmt.Errorf("failed to get node: %w", err)
	}
	node, err := bytesToNode(id, data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node: %w", err)
	}
	n.cache.Put(id, node)
	return node, nil
}

// Put adds a node to the cache. It does not write the node to the store.
func (n *Nodestore) Put(node Node) (uint64, error) {
	id := n.generateNodeID(node)
	n.cache.Put(id, node)
	return id, nil
}

// Flush writes a cached node to the store.
func (n *Nodestore) Flush(id uint64) error {
	node, ok := n.cache.Get(id)
	if !ok {
		return ErrNodeNotFound
	}
	bytes, err := node.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize node: %w", err)
	}
	err = n.store.Put(id, bytes)
	if err != nil {
		return fmt.Errorf("failed to flush node %d: %w", id, err)
	}
	return nil
}

// NewNodestore creates a new nodestore.
func NewNodestore(store storage.Provider, cache *util.LRU[Node]) *Nodestore {
	return &Nodestore{
		store:      store,
		cache:      cache,
		nextNodeID: 1,
		maxNodeID:  1e6, // todo: grab this from persistent storage on startup
	}
}
