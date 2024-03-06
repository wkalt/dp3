package nodestore_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

func TestNodestoreErrors(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	wal, err := nodestore.NewSQLWAL(ctx, db)
	require.NoError(t, err)
	ns := nodestore.NewNodestore(store, cache, wal)
	t.Run("get non-existent node", func(t *testing.T) {
		_, err := ns.Get(ctx, nodestore.NodeID{})
		assert.ErrorIs(t, err, nodestore.ErrNodeNotFound)
	})
	t.Run("flush a nonexistent node", func(t *testing.T) {
		_, err := ns.Flush(ctx, nodestore.NodeID{})
		assert.ErrorIs(t, err, nodestore.ErrNodeNotStaged)
	})
}

func TestNodeStore(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	wal, err := nodestore.NewSQLWAL(ctx, db)
	ns := nodestore.NewNodestore(store, cache, wal)
	t.Run("store and retrieve an inner node", func(t *testing.T) {
		node := nodestore.NewInnerNode(0, 10, 20, 64)
		nodeID, err := ns.Stage(node)
		require.NoError(t, err)
		rootID, err := ns.Flush(ctx, nodeID)
		require.NoError(t, err)
		retrieved, err := ns.Get(ctx, rootID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve a leaf node", func(t *testing.T) {
		node := nodestore.NewLeafNode(nil)
		nodeID, err := ns.Stage(node)
		require.NoError(t, err)
		rootID, err := ns.Flush(ctx, nodeID)
		require.NoError(t, err)
		retrieved, err := ns.Get(ctx, rootID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve inner node that has been evicted from cache", func(t *testing.T) {
		node := nodestore.NewInnerNode(0, 90, 100, 64)
		tmpid, err := ns.Stage(node)
		require.NoError(t, err)
		rootID, err := ns.Flush(ctx, tmpid)
		require.NoError(t, err)
		cache.Reset()
		retrieved, err := ns.Get(ctx, rootID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve leaf node that has been evicted from cache", func(t *testing.T) {
		node := nodestore.NewLeafNode(nil)
		nodeID, err := ns.Stage(node)
		require.NoError(t, err)
		rootID, err := ns.Flush(ctx, nodeID)
		require.NoError(t, err)
		cache.Reset()
		retrieved, err := ns.Get(ctx, rootID)
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

		_, err = ns.Flush(ctx, rootID, inner1ID, inner2ID, leafID)
		require.NoError(t, err)

	})
}
