package nodestore

import (
	"errors"

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
		node := &LeafNode{}
		if err := node.FromBytes(value); err != nil {
			return nil, err
		}
		return node, nil
	}
	node := &InnerNode{}
	if err := node.FromBytes(value); err != nil {
		return nil, err
	}
	return node, nil
}

func (n *Nodestore) Get(id uint64) (Node, error) {
	if value, ok := n.cache.Get(id); ok {
		return value, nil
	}
	data, err := n.store.Get(id)
	if err != nil {
		return nil, err
	}
	node, err := bytesToNode(id, data)
	if err != nil {
		return nil, err
	}
	n.cache.Put(id, node)
	return node, nil
}

func (n *Nodestore) Put(node Node) (uint64, error) {
	id := n.generateNodeID(node)
	n.cache.Put(id, node)
	return id, nil
}

func (n *Nodestore) Flush(id uint64) error {
	node, ok := n.cache.Get(id)
	if !ok {
		return errors.New("not found")
	}
	return n.store.Put(id, node.ToBytes())
}

func New(store storage.Provider, cache *util.LRU[Node]) *Nodestore {
	return &Nodestore{
		store:      store,
		cache:      cache,
		nextNodeID: 1,
		maxNodeID:  1e6, // todo: grab this from persistent storage on startup
	}
}
