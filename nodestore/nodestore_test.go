package nodestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

func TestNodestoreErrors(t *testing.T) {
	store := storage.NewMemStore()
	cache := util.NewLRU[Node](1e6)
	ns := New(store, cache)
	t.Run("get non-existent node", func(t *testing.T) {
		_, err := ns.Get(0)
		assert.ErrorIs(t, err, ErrNodeNotFound)
	})
	t.Run("flush a nonexistent node", func(t *testing.T) {
		assert.ErrorIs(t, ns.Flush(0), ErrNodeNotFound)
	})
	t.Run("read corrupted leaf node from store", func(t *testing.T) {
		nodeID, err := ns.Put(&LeafNode{})
		assert.Nil(t, err)
		assert.Nil(t, ns.Flush(nodeID))
		cache.Reset()
		store.Put(nodeID, []byte{0, 0, 0, 0})
		_, err = ns.Get(nodeID)
		assert.NotNil(t, err)
	})
	t.Run("read corrupted inner node from store", func(t *testing.T) {
		nodeID, err := ns.Put(&InnerNode{})
		assert.Nil(t, err)
		assert.Nil(t, ns.Flush(nodeID))
		cache.Reset()
		store.Put(nodeID, []byte{0, 0, 0, 0})
		_, err = ns.Get(nodeID)
		assert.NotNil(t, err)
	})
}

func TestNodeStore(t *testing.T) {
	store := storage.NewMemStore()
	cache := util.NewLRU[Node](1e6)
	ns := New(store, cache)
	t.Run("store and retrieve an inner node", func(t *testing.T) {
		node := NewInnerNode(10, 20, 1, 64)
		nodeID, err := ns.Put(node)
		assert.Nil(t, err)
		retrieved, err := ns.Get(nodeID)
		assert.Nil(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve a leaf node", func(t *testing.T) {
		node := NewLeafNode(1, nil)
		nodeID, err := ns.Put(node)
		assert.Nil(t, err)
		retrieved, err := ns.Get(nodeID)
		assert.Nil(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve inner node that has been evicted from cache", func(t *testing.T) {
		node := NewInnerNode(90, 100, 1, 64)
		nodeID, err := ns.Put(node)
		assert.Nil(t, err)
		assert.Nil(t, ns.Flush(nodeID))
		cache.Reset()
		retrieved, err := ns.Get(nodeID)
		assert.Nil(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve leaf node that has been evicted from cache", func(t *testing.T) {
		node := &LeafNode{
			Version: 2,
		}
		nodeID, err := ns.Put(node)
		assert.Nil(t, err)
		assert.Nil(t, ns.Flush(nodeID))
		cache.Reset()
		retrieved, err := ns.Get(nodeID)
		assert.Nil(t, err)
		assert.Equal(t, node, retrieved)
	})
}
