package nodestore_test

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/tree"
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

func makeTestNodestore(t *testing.T) *nodestore.Nodestore {
	t.Helper()
	ctx := context.Background()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	wal, err := nodestore.NewSQLWAL(ctx, db)
	require.NoError(t, err)
	return nodestore.NewNodestore(store, cache, wal)
}

func TestWALMerge(t *testing.T) {
	ctx := context.Background()
	ns := makeTestNodestore(t)
	root, err := ns.NewRoot(
		ctx,
		util.DateSeconds("1970-01-01"),
		util.DateSeconds("2030-01-01"),
		60,
		64,
	)
	require.NoError(t, err)

	// inserts go into the staging area
	buf1 := &bytes.Buffer{}
	mcap.WriteFile(t, buf1, []uint64{50 * 1e9})
	root1, path, err := tree.Insert(ctx, ns, root, 1, 50*1e9, buf1.Bytes())
	require.NoError(t, err)
	require.NoError(t, ns.WALFlush(ctx, "my-stream", 2, path))

	buf2 := &bytes.Buffer{}
	mcap.WriteFile(t, buf2, []uint64{70 * 1e9})
	root2, path2, err := tree.Insert(ctx, ns, root1, 1, 70*1e9, buf2.Bytes())
	require.NoError(t, err)
	require.NoError(t, ns.WALFlush(ctx, "my-stream", 3, path2))

	buf3 := &bytes.Buffer{}
	mcap.WriteFile(t, buf3, []uint64{70 * 1e9})
	root3, path3, err := tree.Insert(ctx, ns, root2, 1, 70*1e9, buf3.Bytes())
	require.NoError(t, err)
	require.NoError(t, ns.WALFlush(ctx, "my-stream", 3, path3))

	_, err = ns.WALMerge(ctx, 4, []nodestore.NodeID{root1, root2, root3})
	require.NoError(t, err)
}

func TestNewRoot(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	wal, err := nodestore.NewSQLWAL(ctx, db)
	require.NoError(t, err)
	ns := nodestore.NewNodestore(store, cache, wal)
	root, err := ns.NewRoot(
		ctx,
		util.DateSeconds("1970-01-01"),
		util.DateSeconds("2030-01-01"),
		60,
		64,
	)
	require.NoError(t, err)
	_, err = ns.Get(ctx, root)
	require.NoError(t, err)
}

func TestNodeStore(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	wal, err := nodestore.NewSQLWAL(ctx, db)
	require.NoError(t, err)
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
