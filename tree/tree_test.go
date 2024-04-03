package tree_test

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
)

func TestTreeErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("inserting into a non-existent root", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		tw.SetRoot(nodestore.RandomNodeID())
		stats := map[string]*nodestore.Statistics{}
		err := tree.Insert(ctx, tw, 0, 0, []byte{0x01}, stats)
		require.ErrorIs(t, err, nodestore.NodeNotFoundError{})
	})

	t.Run("root is nil", func(t *testing.T) {
		tw := tree.NewMemTree(nodestore.RandomNodeID(), nil)
		stats := map[string]*nodestore.Statistics{}
		require.Error(t, tree.Insert(ctx, tw, 0, 0, []byte{0x01}, stats))
	})

	t.Run("out of bounds insert", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		stats := map[string]*nodestore.Statistics{}
		err := tree.Insert(ctx, tw, 0, 10000*1e9, []byte{0x01}, stats)
		require.ErrorIs(t, err, tree.OutOfBoundsError{})
	})

	t.Run("root is not an inner node", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		stats := map[string]*nodestore.Statistics{}
		require.NoError(t, tree.Insert(ctx, tw, 0, 1000*1e9, []byte{0x01}, stats))

		leaf := nodestore.NewLeafNode([]byte{0x01}, nil, nil)
		leafid := nodestore.RandomNodeID()
		require.NoError(t, tw.Put(ctx, leafid, leaf))
		tw.SetRoot(leafid)

		err := tree.Insert(ctx, tw, 0, 64*1e9, []byte{0x01}, stats)
		require.ErrorIs(t, err, tree.UnexpectedNodeError{})
	})
}

func TestMergeErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("merging trees of different height", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		require.NoError(t, tree.Insert(ctx, tw, 0, 1000*1e9, []byte{0x01}, nil))

		root2 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw2 := tree.NewMemTree(nodestore.RandomNodeID(), root2)
		require.NoError(t, tree.Insert(ctx, tw2, 0, 64*1e9, []byte{0x01}, nil))

		root3 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw3 := tree.NewMemTree(nodestore.RandomNodeID(), root3)

		err := tree.Merge(ctx, tw3, tw3, tw, tw2)
		require.ErrorIs(t, err, tree.MismatchedHeightsError{})
	})

	t.Run("merging tree with wrong height", func(t *testing.T) {
		// build a corrupt tree
		root := nodestore.NewInnerNode(2, 0, 64*64*64, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)

		// child
		childnode := nodestore.NewInnerNode(1, 0, 64*64, 64)
		childid := nodestore.RandomNodeID()
		child := nodestore.Child{
			ID:         childid,
			Version:    0,
			Statistics: nil,
		}
		require.NoError(t, tw.Put(ctx, childid, childnode))
		root.Children[0] = &child

		// grandchild should be a leaf, based on tree dimensions
		gchildnode := nodestore.NewInnerNode(0, 0, 64*64, 64)
		gchildid := nodestore.RandomNodeID()
		gchild := nodestore.Child{
			ID:         gchildid,
			Version:    0,
			Statistics: nil,
		}
		require.NoError(t, tw.Put(ctx, gchildid, gchildnode))
		childnode.Children[0] = &gchild

		// merge with a correct tree
		root2 := nodestore.NewInnerNode(2, 0, 64*64*64, 64)
		tw2 := tree.NewMemTree(nodestore.RandomNodeID(), root2)
		require.NoError(t, tree.Insert(ctx, tw2, 0, 63*1e9, []byte{0x01}, nil))

		root3 := nodestore.NewInnerNode(2, 0, 64*64*64, 64)
		tw3 := tree.NewMemTree(nodestore.RandomNodeID(), root3)

		err := tree.Merge(ctx, tw3, tw3, tw, tw2)
		require.Error(t, err)
	})

	t.Run("root is not an inner node", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		require.NoError(t, tree.Insert(ctx, tw, 0, 1000*1e9, []byte{0x01}, nil))

		leaf := nodestore.NewLeafNode([]byte{0x01}, nil, nil)
		leafid := nodestore.RandomNodeID()
		require.NoError(t, tw.Put(ctx, leafid, leaf))
		tw.SetRoot(leafid)

		root2 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw2 := tree.NewMemTree(nodestore.RandomNodeID(), root2)
		require.NoError(t, tree.Insert(ctx, tw2, 0, 64*1e9, []byte{0x01}, nil))

		root3 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw3 := tree.NewMemTree(nodestore.RandomNodeID(), root3)

		err := tree.Merge(ctx, tw3, tw3, tw, tw2)
		require.ErrorIs(t, err, tree.UnexpectedNodeError{})
	})

	t.Run("root does not exist", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		require.NoError(t, tree.Insert(ctx, tw, 0, 1000*1e9, []byte{0x01}, nil))
		tw.SetRoot(nodestore.RandomNodeID())

		root2 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw2 := tree.NewMemTree(nodestore.RandomNodeID(), root2)

		err := tree.Merge(ctx, tw2, tw2, tw)
		require.ErrorIs(t, err, nodestore.NodeNotFoundError{})
	})
}

func TestTreeInsert(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		height    uint8
		times     [][]int64
		repr      string
	}{
		{
			"empty tree",
			1,
			[][]int64{},
			"[0-4096]",
		},
		{
			"single insert",
			1,
			[][]int64{{10}},
			"[0-4096 [0-64:1 (1b count=1) [leaf 1 msg]]]",
		},
		{
			"height 2",
			2,
			[][]int64{{10}},
			"[0-262144 [0-4096:1 (1b count=1) [0-64:1 (1b count=1) [leaf 1 msg]]]]",
		},
		{
			"two inserts into same bucket are merged",
			1,
			[][]int64{{10}, {20}},
			"[0-4096 [0-64:2 (1b count=2) [leaf 2 msgs]]]",
		},
		{
			"inserts into different buckets",
			1,
			[][]int64{{10}, {100}, {256}, {1000}},
			`[0-4096 [0-64:1 (1b count=1) [leaf 1 msg]] [64-128:2 (1b count=1) [leaf 1 msg]]
			[256-320:3 (1b count=1) [leaf 1 msg]] [960-1024:4 (1b count=1) [leaf 1 msg]]]`,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			output := tree.MergeInserts(ctx, t, 0, util.Pow(uint64(64), int(c.height+1)), c.height, 64, c.times)
			repr, err := tree.Print(ctx, output)
			require.NoError(t, err)
			assertEqualTrees(t, c.repr, repr)
		})
	}
}

func assertEqualTrees(t *testing.T, a, b string) {
	t.Helper()
	require.Equal(t, removeSpace(a), removeSpace(b), "%s != %s", a, b)
}

func removeSpace(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "  ", "")
	s = strings.ReplaceAll(s, "\t", "")
	return s
}

func TestStatRange(t *testing.T) {
	ctx := context.Background()
	testhash := "1ba234e59378bc656d587c45c4191bfc24c2c657e871f148faa552350738c470"
	cases := []struct {
		assertion   string
		messages    [][]int64
		start       uint64
		end         uint64
		granularity uint64
		expected    []nodestore.StatRange
	}{
		{
			"empty tree",
			[][]int64{},
			0,
			100e9,
			1e9,
			[]nodestore.StatRange{},
		},
		{
			"single insert",
			[][]int64{{100}},
			0,
			100e9,
			600e9,
			[]nodestore.StatRange{
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "byteCount", int64(0)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "minObservedTime", int64(100)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "maxObservedTime", int64(100)),
			},
		},
		{
			"inserts spanning buckets",
			[][]int64{{100}, {4097}},
			0,
			5000e9,
			600e9,
			[]nodestore.StatRange{
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "byteCount", int64(0)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "minObservedTime", int64(4097)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "maxObservedTime", int64(4097)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "byteCount", int64(0)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "minObservedTime", int64(100)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "maxObservedTime", int64(100)),
			},
		},
		{
			"inserts spanning disjoint buckets",
			[][]int64{{100}, {262143}},
			0,
			math.MaxInt64,
			600e9,
			[]nodestore.StatRange{
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "byteCount", int64(0)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "minObservedTime", int64(262143)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "maxObservedTime", int64(262143)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "byteCount", int64(0)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "minObservedTime", int64(100)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "maxObservedTime", int64(100)),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			tw := tree.MergeInserts(ctx, t, 0, 64*64*64, 2, 64, c.messages)
			statrange, err := tree.GetStatRange(ctx, tw, c.start, c.end, c.granularity)
			require.NoError(t, err)
			require.Equal(t, c.expected, statrange)
		})
	}
}

func TestMerge(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		height    uint8
		prep      [][]int64
		inputs    [][]int64
		expected  string
	}{
		{
			"merge into empty tree",
			1,
			[][]int64{},
			[][]int64{{100}},
			"[0-4096 [64-128:1 (1b count=1) [leaf 1 msg]]]",
		},
		{
			"merge into populated tree, nonoverlapping",
			1,
			[][]int64{{33}},
			[][]int64{{100}},
			"[0-4096 [<link> 0-64:1 (1b count=1) [leaf 1 msg]] [64-128:1 (1b count=1) [leaf 1 msg]]]",
		},
		{
			"merge into populated tree, overlapping",
			1,
			[][]int64{{33}, {120}},
			[][]int64{{100}},
			"[0-4096 [<link> 0-64:1 (1b count=1) [leaf 1 msg]] [64-128:1 (1b count=2) [leaf 1 msg]->[leaf 1 msg]]]",
		},
		{
			"merge into a populated tree, multiple overlapping",
			1,
			[][]int64{{33}, {120}},
			[][]int64{{39}, {121}},
			"[0-4096 [0-64:1 (1b count=2) [leaf 1 msg]->[leaf 1 msg]] [64-128:2 (1b count=2) [leaf 1 msg]->[leaf 1 msg]]]",
		},
		{
			"depth 2",
			2,
			[][]int64{{33}, {120}},
			[][]int64{{1000}},
			`[0-262144 [0-4096:1 (1b count=3) [<link> 0-64:1 (1b count=1) [leaf 1 msg]]
			[<link> 64-128:2 (1b count=1) [leaf 1 msg]] [960-1024:1 (1b count=1) [leaf 1 msg]]]]`,
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			base := tree.MergeInserts(
				ctx, t, 0, util.Pow(uint64(64), int(c.height+1)), c.height, 64, c.prep,
			)
			partial := tree.MergeInserts(
				ctx, t, 0, util.Pow(uint64(64), int(c.height+1)), c.height, 64, c.inputs,
			)
			rootnode := nodestore.NewInnerNode(c.height, 0, util.Pow(uint64(64), int(c.height+1)), 64)
			overlay := tree.NewMemTree(nodestore.RandomNodeID(), rootnode)
			require.NoError(t, tree.Merge(ctx, overlay, base, partial))
			repr, err := tree.Print(ctx, overlay, base)
			require.NoError(t, err)
			assertEqualTrees(t, c.expected, repr)
		})
	}
}
