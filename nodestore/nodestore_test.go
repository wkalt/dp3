package nodestore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

func TestNodestoreErrors(t *testing.T) {
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.Node](1e6)
	ns := nodestore.NewNodestore(store, cache)
	t.Run("get non-existent node", func(t *testing.T) {
		_, err := ns.Get(0)
		assert.ErrorIs(t, err, nodestore.ErrNodeNotFound)
	})
	t.Run("flush a nonexistent node", func(t *testing.T) {
		assert.ErrorIs(t, ns.Flush(0), nodestore.ErrNodeNotFound)
	})
	t.Run("read corrupted leaf node from store", func(t *testing.T) {
		node := nodestore.NewLeafNode(1, nil)
		nodeID, err := ns.Put(node)
		require.NoError(t, err)
		require.NoError(t, ns.Flush(nodeID))
		cache.Reset()
		require.NoError(t, store.Put(nodeID, []byte{0, 0, 0, 0}))
		_, err = ns.Get(nodeID)
		assert.Error(t, err)
	})
	t.Run("read corrupted inner node from store", func(t *testing.T) {
		node := nodestore.NewInnerNode(0, 0, 0, 0)
		nodeID, err := ns.Put(node)
		require.NoError(t, err)
		require.NoError(t, ns.Flush(nodeID))
		cache.Reset()
		require.NoError(t, store.Put(nodeID, []byte{0, 0, 0, 0}))
		_, err = ns.Get(nodeID)
		assert.Error(t, err)
	})
}

func TestNodeStore(t *testing.T) {
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.Node](1e6)
	ns := nodestore.NewNodestore(store, cache)
	t.Run("store and retrieve an inner node", func(t *testing.T) {
		node := nodestore.NewInnerNode(10, 20, 1, 64)
		nodeID, err := ns.Put(node)
		require.NoError(t, err)
		retrieved, err := ns.Get(nodeID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve a leaf node", func(t *testing.T) {
		node := nodestore.NewLeafNode(1, nil)
		nodeID, err := ns.Put(node)
		require.NoError(t, err)
		retrieved, err := ns.Get(nodeID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve inner node that has been evicted from cache", func(t *testing.T) {
		node := nodestore.NewInnerNode(90, 100, 1, 64)
		nodeID, err := ns.Put(node)
		require.NoError(t, err)
		require.NoError(t, ns.Flush(nodeID))
		cache.Reset()
		retrieved, err := ns.Get(nodeID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve leaf node that has been evicted from cache", func(t *testing.T) {
		node := nodestore.NewLeafNode(2, nil)
		nodeID, err := ns.Put(node)
		require.NoError(t, err)
		require.NoError(t, ns.Flush(nodeID))
		cache.Reset()
		retrieved, err := ns.Get(nodeID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
}
