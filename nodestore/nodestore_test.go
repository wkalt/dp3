package nodestore_test

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
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
	t.Run("get nonexistent node", func(t *testing.T) {
		id := nodestore.RandomNodeID()
		_, err := ns.Get(ctx, id)
		assert.ErrorIs(t, err, nodestore.NodeNotFoundError{id})
	})
	t.Run("flush a nonexistent node", func(t *testing.T) {
		_, err := ns.Flush(ctx, 1, nodestore.NodeID{})
		assert.ErrorIs(t, err, nodestore.ErrNodeNotStaged)
	})
}

func removeSpace(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "  ", "")
	s = strings.ReplaceAll(s, "\t", "")
	return s
}

func assertEqualTrees(t *testing.T, a, b string) {
	t.Helper()
	require.Equal(t, removeSpace(a), removeSpace(b), "%s != %s", a, b)
}

func makeTestNodestore(t *testing.T) *nodestore.Nodestore {
	t.Helper()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
	wal := nodestore.NewMemWAL()
	return nodestore.NewNodestore(store, cache, wal)
}

func TestWALMerge(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		root      []string
		nodes     [][]string
		expected  string
	}{
		{
			"single node into empty root",
			[]string{},
			[][]string{{"1970-01-03"}},
			"[0-707788800:2 [0-11059200:2 (count=1) [172800-345600:2 (count=1) [leaf 1 msg]]]]",
		},
		{
			"single node into populated, nonoverlapping root",
			[]string{"1970-01-01"},
			[][]string{{"1970-01-03"}},
			`[0-707788800:3 [0-11059200:3 (count=2) [0-172800:1 (count=1) [leaf 1 msg]]
			[172800-345600:3 (count=1) [leaf 1 msg]]]]`,
		},
		{
			"two nonoverlapping nodes into empty root",
			[]string{},
			[][]string{{"1970-01-03"}, {"1970-01-05"}},
			`[0-707788800:3 [0-11059200:3 (count=2) [172800-345600:3 (count=1) [leaf 1 msg]]
			[345600-518400:3 (count=1) [leaf 1 msg]]]]`,
		},
		{
			"two nonoverlapping nodes into nonempty empty root",
			[]string{"1970-01-01"},
			[][]string{{"1970-01-03"}, {"1970-01-05"}},
			`[0-707788800:4 [0-11059200:4 (count=3) [0-172800:1 (count=1) [leaf 1 msg]]
			[172800-345600:4 (count=1) [leaf 1 msg]] [345600-518400:4 (count=1) [leaf 1 msg]]]]`,
		},
		{
			"overlapping nodes into empty root",
			[]string{},
			[][]string{{"1970-01-01"}, {"1970-01-02"}},
			"[0-707788800:3 [0-11059200:3 (count=2) [0-172800:3 (count=2) [leaf 2 msgs]]]]",
		},
		{
			"overlapping nodes into nonempty root",
			[]string{"1970-01-01"},
			[][]string{{"1970-01-01"}, {"1970-01-02"}},
			"[0-707788800:4 [0-11059200:4 (count=3) [0-172800:4 (count=3) [leaf 3 msgs]]]]",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ns := makeTestNodestore(t)
			rootID, err := ns.NewRoot(
				ctx,
				util.DateSeconds("1970-01-01"),
				util.DateSeconds("1975-01-01"),
				60*60*24*2, // five years, two day buckets
				64,
			)
			require.NoError(t, err)
			version := uint64(1)
			if len(c.root) > 0 {
				var nodeIDs []nodestore.NodeID
				buf := &bytes.Buffer{}

				secs := util.DateSeconds(c.root[0])
				nsecs := secs * 1e9
				mcap.WriteFile(t, buf, []uint64{nsecs})
				stats := &nodestore.Statistics{
					MessageCount: uint64(len(c.root)),
				}
				_, nodeIDs, err = tree.Insert(ctx, ns, rootID, 1, nsecs, buf.Bytes(), stats)
				require.NoError(t, err)
				rootID, err = ns.Flush(ctx, version, nodeIDs...)
				require.NoError(t, err)
				version++
			}

			producer := "producer"
			topic := "topic"
			roots := make([]nodestore.NodeID, len(c.nodes))
			for i, node := range c.nodes {
				buf := &bytes.Buffer{}

				times := []uint64{}
				for _, time := range node {
					times = append(times, 1e9*util.DateSeconds(time))
				}
				mcap.WriteFile(t, buf, times)
				stats := &nodestore.Statistics{
					MessageCount: uint64(len(node)),
				}
				newRootID, nodeIDs, err := tree.Insert(
					ctx, ns, rootID, version, times[0], buf.Bytes(), stats) // into staging
				require.NoError(t, err)
				require.NoError(t, ns.FlushStagingToWAL(ctx, producer, topic, version, nodeIDs)) // staging -> wal
				version++
				roots[i] = newRootID
			}

			rootID, err = ns.MergeWALToStorage(ctx, rootID, version, roots)
			require.NoError(t, err)

			str, err := ns.Print(ctx, rootID, version, nil)
			require.NoError(t, err)
			assertEqualTrees(t, c.expected, str)
		})
	}
}

func TestNewRoot(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
	wal := nodestore.NewMemWAL()
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
		nodeID := ns.Stage(node)
		rootID, err := ns.Flush(ctx, 1, nodeID)
		require.NoError(t, err)
		retrieved, err := ns.Get(ctx, rootID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve a leaf node", func(t *testing.T) {
		node := nodestore.NewLeafNode(nil)
		nodeID := ns.Stage(node)
		rootID, err := ns.Flush(ctx, 1, nodeID)
		require.NoError(t, err)
		retrieved, err := ns.Get(ctx, rootID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve inner node that has been evicted from cache", func(t *testing.T) {
		node := nodestore.NewInnerNode(0, 90, 100, 64)
		tmpid := ns.Stage(node)
		rootID, err := ns.Flush(ctx, 1, tmpid)
		require.NoError(t, err)
		cache.Reset()
		retrieved, err := ns.Get(ctx, rootID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("store and retrieve leaf node that has been evicted from cache", func(t *testing.T) {
		node := nodestore.NewLeafNode(nil)
		nodeID := ns.Stage(node)
		rootID, err := ns.Flush(ctx, 1, nodeID)
		require.NoError(t, err)
		cache.Reset()
		retrieved, err := ns.Get(ctx, rootID)
		require.NoError(t, err)
		assert.Equal(t, node, retrieved)
	})
	t.Run("flush a set of nodes", func(t *testing.T) {
		root := nodestore.NewInnerNode(0, 0, 0, 1)
		rootID := ns.Stage(root)
		inner1 := nodestore.NewInnerNode(0, 1, 1, 1)
		inner1ID := ns.Stage(inner1)
		inner2 := nodestore.NewInnerNode(0, 2, 2, 1)
		inner2ID := ns.Stage(inner2)
		leaf := nodestore.NewLeafNode([]byte("leaf1"))
		leafID := ns.Stage(leaf)

		stats := &nodestore.Statistics{
			MessageCount: 1,
		}

		root.PlaceChild(0, inner1ID, 1, stats)
		inner1.PlaceChild(0, inner2ID, 1, stats)
		inner2.PlaceChild(0, leafID, 1, stats)

		_, err = ns.Flush(ctx, 1, rootID, inner1ID, inner2ID, leafID)
		require.NoError(t, err)
	})
}
