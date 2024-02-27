package nodestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

func TestNodeStore(t *testing.T) {
	store := storage.NewMemStore()
	cache := util.NewLRU[Node](1e6)
	ns := New(store, cache)
	t.Run("store and retrieve an inner node", func(t *testing.T) {
		node := &InnerNode{
			Start: 10,
			End:   20,
		}
		nodeID, err := ns.Put(node)
		assert.Nil(t, err)
		retrieved, err := ns.Get(nodeID)
		assert.Nil(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve a leaf node", func(t *testing.T) {
		node := &LeafNode{
			Version: 1,
		}
		nodeID, err := ns.Put(node)
		assert.Nil(t, err)
		retrieved, err := ns.Get(nodeID)
		assert.Nil(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve a node that has been evicted from cache", func(t *testing.T) {
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
