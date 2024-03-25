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
		err := tree.Insert(ctx, tw, 0, 0, []byte{0x01}, &nodestore.Statistics{})
		require.ErrorIs(t, err, nodestore.NodeNotFoundError{})
	})

	t.Run("root is nil", func(t *testing.T) {
		tw := tree.NewMemTree(nodestore.RandomNodeID(), nil)
		require.Error(t, tree.Insert(ctx, tw, 0, 0, []byte{0x01}, &nodestore.Statistics{}))
	})

	t.Run("out of bounds insert", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		err := tree.Insert(ctx, tw, 0, 10000*1e9, []byte{0x01}, &nodestore.Statistics{})
		require.ErrorIs(t, err, tree.OutOfBoundsError{})
	})

	t.Run("root is not an inner node", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		require.NoError(t, tree.Insert(ctx, tw, 0, 1000*1e9, []byte{0x01}, &nodestore.Statistics{}))

		leaf := nodestore.NewLeafNode([]byte{0x01})
		leafid := nodestore.RandomNodeID()
		require.NoError(t, tw.Put(ctx, leafid, leaf))
		tw.SetRoot(leafid)

		err := tree.Insert(ctx, tw, 0, 64*1e9, []byte{0x01}, &nodestore.Statistics{})
		require.ErrorIs(t, err, tree.UnexpectedNodeError{})
	})
}

func TestMergeErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("merging trees of different height", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		require.NoError(t, tree.Insert(ctx, tw, 0, 1000*1e9, []byte{0x01}, &nodestore.Statistics{}))

		root2 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw2 := tree.NewMemTree(nodestore.RandomNodeID(), root2)
		require.NoError(t, tree.Insert(ctx, tw2, 0, 64*1e9, []byte{0x01}, &nodestore.Statistics{}))

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
			Statistics: &nodestore.Statistics{},
		}
		require.NoError(t, tw.Put(ctx, childid, childnode))
		root.Children[0] = &child

		// grandchild should be a leaf, based on tree dimensions
		gchildnode := nodestore.NewInnerNode(0, 0, 64*64, 64)
		gchildid := nodestore.RandomNodeID()
		gchild := nodestore.Child{
			ID:         gchildid,
			Version:    0,
			Statistics: &nodestore.Statistics{},
		}
		require.NoError(t, tw.Put(ctx, gchildid, gchildnode))
		childnode.Children[0] = &gchild

		// merge with a correct tree
		root2 := nodestore.NewInnerNode(2, 0, 64*64*64, 64)
		tw2 := tree.NewMemTree(nodestore.RandomNodeID(), root2)
		require.NoError(t, tree.Insert(ctx, tw2, 0, 63*1e9, []byte{0x01}, &nodestore.Statistics{}))

		root3 := nodestore.NewInnerNode(2, 0, 64*64*64, 64)
		tw3 := tree.NewMemTree(nodestore.RandomNodeID(), root3)

		err := tree.Merge(ctx, tw3, tw3, tw, tw2)
		require.Error(t, err)
	})

	t.Run("root is not an inner node", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		require.NoError(t, tree.Insert(ctx, tw, 0, 1000*1e9, []byte{0x01}, &nodestore.Statistics{}))

		leaf := nodestore.NewLeafNode([]byte{0x01})
		leafid := nodestore.RandomNodeID()
		require.NoError(t, tw.Put(ctx, leafid, leaf))
		tw.SetRoot(leafid)

		root2 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw2 := tree.NewMemTree(nodestore.RandomNodeID(), root2)
		require.NoError(t, tree.Insert(ctx, tw2, 0, 64*1e9, []byte{0x01}, &nodestore.Statistics{}))

		root3 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw3 := tree.NewMemTree(nodestore.RandomNodeID(), root3)

		err := tree.Merge(ctx, tw3, tw3, tw, tw2)
		require.ErrorIs(t, err, tree.UnexpectedNodeError{})
	})

	t.Run("root does not exist", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw := tree.NewMemTree(nodestore.RandomNodeID(), root)
		require.NoError(t, tree.Insert(ctx, tw, 0, 1000*1e9, []byte{0x01}, &nodestore.Statistics{}))
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
		times     [][]uint64
		repr      string
	}{
		{
			"empty tree",
			1,
			[][]uint64{},
			"[0-4096]",
		},
		{
			"single insert",
			1,
			[][]uint64{{10}},
			"[0-4096 [0-64:1 (count=1) [leaf 1 msg]]]",
		},
		{
			"height 2",
			2,
			[][]uint64{{10}},
			"[0-262144 [0-4096:1 (count=1) [0-64:1 (count=1) [leaf 1 msg]]]]",
		},
		{
			"two inserts into same bucket are merged",
			1,
			[][]uint64{{10}, {20}},
			"[0-4096 [0-64:2 (count=2) [leaf 2 msgs]]]",
		},
		{
			"inserts into different buckets",
			1,
			[][]uint64{{10}, {100}, {256}, {1000}},
			`[0-4096 [0-64:1 (count=1) [leaf 1 msg]] [64-128:2 (count=1) [leaf 1 msg]]
			[256-320:3 (count=1) [leaf 1 msg]] [960-1024:4 (count=1) [leaf 1 msg]]]`,
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
	cases := []struct {
		assertion   string
		messages    [][]uint64
		start       uint64
		end         uint64
		granularity uint64
		expected    []tree.StatRange
	}{
		{
			"empty tree",
			[][]uint64{},
			0,
			100e9,
			1e9,
			[]tree.StatRange{},
		},
		{
			"single insert",
			[][]uint64{{100}},
			0,
			100e9,
			600e9,
			[]tree.StatRange{
				{
					Start:      64e9,
					End:        128e9,
					Statistics: &nodestore.Statistics{MessageCount: 1},
				},
			},
		},
		{
			"inserts spanning buckets",
			[][]uint64{{100}, {4097}},
			0,
			5000e9,
			600e9,
			[]tree.StatRange{
				{Start: 4096000000000, End: 4160000000000, Statistics: &nodestore.Statistics{MessageCount: 1}},
				{Start: 64000000000, End: 128000000000, Statistics: &nodestore.Statistics{MessageCount: 1}},
			},
		},
		{
			"inserts spanning disjoint buckets",
			[][]uint64{{100}, {262143}},
			0,
			math.MaxUint64,
			600e9,
			[]tree.StatRange{
				{Start: 262080000000000, End: 262144000000000, Statistics: &nodestore.Statistics{MessageCount: 1}},
				{Start: 64000000000, End: 128000000000, Statistics: &nodestore.Statistics{MessageCount: 1}},
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
		prep      [][]uint64
		inputs    [][]uint64
		expected  string
	}{
		{
			"merge into empty tree",
			1,
			[][]uint64{},
			[][]uint64{{100}},
			"[0-4096 [64-128:1 (count=1) [leaf 1 msg]]]",
		},
		{
			"merge into populated tree, nonoverlapping",
			1,
			[][]uint64{{33}},
			[][]uint64{{100}},
			"[0-4096 [<link> 0-64:1 (count=1) [leaf 1 msg]] [64-128:1 (count=1) [leaf 1 msg]]]",
		},
		{
			"merge into populated tree, overlapping",
			1,
			[][]uint64{{33}, {120}},
			[][]uint64{{100}},
			"[0-4096 [<link> 0-64:1 (count=1) [leaf 1 msg]] [64-128:1 (count=2) [leaf 2 msgs]]]",
		},
		{
			"merge into a populated tree, multiple overlapping",
			1,
			[][]uint64{{33}, {120}},
			[][]uint64{{39}, {121}},
			"[0-4096 [0-64:1 (count=2) [leaf 2 msgs]] [64-128:2 (count=2) [leaf 2 msgs]]]",
		},
		{
			"depth 2",
			2,
			[][]uint64{{33}, {120}},
			[][]uint64{{1000}},
			`[0-262144 [0-4096:1 (count=3) [<link> 0-64:1 (count=1) [leaf 1 msg]]
			[<link> 64-128:2 (count=1) [leaf 1 msg]] [960-1024:1 (count=1) [leaf 1 msg]]]]`,
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
