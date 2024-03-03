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
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
	ns := nodestore.NewNodestore(store, cache)
	t.Run("get non-existent node", func(t *testing.T) {
		_, err := ns.Get(nodestore.NodeID{})
		assert.ErrorIs(t, err, nodestore.ErrNodeNotFound)
	})
	t.Run("flush a nonexistent node", func(t *testing.T) {
		_, err := ns.Flush(nodestore.NodeID{})
		assert.ErrorIs(t, err, nodestore.ErrNodeNotStaged)
	})
}

func TestNodeStore(t *testing.T) {
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
	ns := nodestore.NewNodestore(store, cache)
	t.Run("store and retrieve an inner node", func(t *testing.T) {
		node := nodestore.NewInnerNode(0, 10, 20, 64)
		nodeID, err := ns.Stage(node)
		require.NoError(t, err)
		nodeIDs, err := ns.Flush(nodeID)
		require.NoError(t, err)
		retrieved, err := ns.Get(nodeIDs[0])
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve a leaf node", func(t *testing.T) {
		node := nodestore.NewLeafNode(nil)
		nodeID, err := ns.Stage(node)
		require.NoError(t, err)
		nodeIDs, err := ns.Flush(nodeID)
		require.NoError(t, err)
		retrieved, err := ns.Get(nodeIDs[0])
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve inner node that has been evicted from cache", func(t *testing.T) {
		node := nodestore.NewInnerNode(0, 90, 100, 64)
		tmpid, err := ns.Stage(node)
		require.NoError(t, err)
		ids, err := ns.Flush(tmpid)
		require.NoError(t, err)
		cache.Reset()
		retrieved, err := ns.Get(ids[0])
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve leaf node that has been evicted from cache", func(t *testing.T) {
		node := nodestore.NewLeafNode(nil)
		nodeID, err := ns.Stage(node)
		require.NoError(t, err)
		nodeIDs, err := ns.Flush(nodeID)
		require.NoError(t, err)
		cache.Reset()
		retrieved, err := ns.Get(nodeIDs[0])
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("flush a set of nodes", func(t *testing.T) {
		root := nodestore.NewInnerNode(0, 0, 0, 1)
		rootID, err := ns.Stage(root)
		require.NoError(t, err)
		inner1 := nodestore.NewInnerNode(0, 1, 1, 1)
		inner1ID, err := ns.Stage(inner1)
		require.NoError(t, err)
		inner2 := nodestore.NewInnerNode(0, 2, 2, 1)
		inner2ID, err := ns.Stage(inner2)
		require.NoError(t, err)
		leaf := nodestore.NewLeafNode([]byte("leaf1"))
		leafID, err := ns.Stage(leaf)
		require.NoError(t, err)

		root.PlaceChild(0, inner1ID, 1)
		inner1.PlaceChild(0, inner2ID, 1)
		inner2.PlaceChild(0, leafID, 1)

		nodeIDs, err := ns.Flush(rootID, inner1ID, inner2ID, leafID)
		require.NoError(t, err)

		// nodes are now traversable with new IDs
		for i, id := range nodeIDs {
			node, err := ns.Get(id)
			require.NoError(t, err)
			switch node := node.(type) {
			case *nodestore.InnerNode:
				assert.Equal(t, node.Children[0].ID, nodeIDs[i+1])
			case *nodestore.LeafNode:
				break
			}
		}
	})
}
