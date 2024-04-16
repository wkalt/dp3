package tree_test

import (
	"context"
	"fmt"
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

	t.Run("root is nil", func(t *testing.T) {
		stats := map[string]*nodestore.Statistics{}
		_, err := tree.Insert(ctx, nil, 0, 0, []byte{0x01}, stats)
		require.Error(t, err)
	})

	t.Run("out of bounds insert", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		stats := map[string]*nodestore.Statistics{}
		_, err := tree.Insert(ctx, root, 0, 10000*1e9, []byte{0x01}, stats)
		require.ErrorIs(t, err, tree.OutOfBoundsError{})
	})

	t.Run("out of bounds delete", func(t *testing.T) {
		rootStart, rootEnd := uint64(0), uint64(4096)
		start, end := uint64(10000*1e9), uint64(10001*1e9)
		root := nodestore.NewInnerNode(1, rootStart, rootEnd, 64)
		_, err := tree.DeleteMessagesInRange(ctx, root, 0, start, end)
		require.EqualError(t, err, fmt.Sprintf(
			"range [%d, %d) out of bounds [%d, %d)",
			start,
			end,
			rootStart*1e9,
			rootEnd*1e9,
		))
	})

	t.Run("start cannot be >= end for delete", func(t *testing.T) {
		start, end := uint64(1000*1e9), uint64(0)
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		_, err := tree.DeleteMessagesInRange(ctx, root, 0, start, end)
		require.EqualError(t, err, fmt.Sprintf("invalid range: start %d cannot be >= end %d", start, end))
	})
}

func TestMergeErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("merging trees of different height", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw, err := tree.Insert(ctx, root, 0, 1000*1e9, []byte{0x01}, nil)
		require.NoError(t, err)

		root2 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw2, err := tree.Insert(ctx, root2, 0, 64*1e9, []byte{0x01}, nil)
		require.NoError(t, err)

		root3 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw3 := tree.NewMemTree(nodestore.RandomNodeID(), root3)

		_, err = tree.Merge(ctx, tw3, tw, tw2)
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
		tw2, err := tree.Insert(ctx, root2, 0, 63*1e9, []byte{0x01}, nil)
		require.NoError(t, err)

		root3 := nodestore.NewInnerNode(2, 0, 64*64*64, 64)
		tw3 := tree.NewMemTree(nodestore.RandomNodeID(), root3)

		_, err = tree.Merge(ctx, tw3, tw, tw2)
		require.Error(t, err)
	})

	t.Run("root is not an inner node", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw, err := tree.Insert(ctx, root, 0, 1000*1e9, []byte{0x01}, nil)
		require.NoError(t, err)

		leaf := nodestore.NewLeafNode([]byte{0x01}, nil, nil)
		leafid := nodestore.RandomNodeID()
		require.NoError(t, tw.Put(ctx, leafid, leaf))
		tw.SetRoot(leafid)

		root2 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw2, err := tree.Insert(ctx, root2, 0, 64*1e9, []byte{0x01}, nil)
		require.NoError(t, err)

		root3 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw3 := tree.NewMemTree(nodestore.RandomNodeID(), root3)

		_, err = tree.Merge(ctx, tw3, tw, tw2)
		require.ErrorIs(t, err, tree.UnexpectedNodeError{})
	})

	t.Run("root does not exist", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw, err := tree.Insert(ctx, root, 0, 1000*1e9, []byte{0x01}, nil)
		require.NoError(t, err)
		tw.SetRoot(nodestore.RandomNodeID())

		root2 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw2 := tree.NewMemTree(nodestore.RandomNodeID(), root2)

		_, err = tree.Merge(ctx, tw2, tw)
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

func TestDeleteMessagesInRange(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		height    uint8
		prep      [][]int64
		start     uint64
		end       uint64

		partialResult string
		mergedResult  string
	}{
		{
			"delete from empty tree inserts tombstones even where no data existed",
			1,
			[][]int64{},
			0,
			8,
			"[0-16 [<del> 0-4:1] [<del> 4-8:1]]",
			"[0-16]",
		},
		{
			"delete from single insert",
			1,
			[][]int64{{6}},
			4,
			8,
			"[0-16 [<del> 4-8:2]]",
			"[0-16]",
		},
		{
			"delete one of two inserts",
			1,
			[][]int64{{3}, {7}},
			0,
			4,
			"[0-16 [<del> 0-4:3]]",
			"[0-16 [<ref> 4-8:2 (1b count=1) [leaf 1 msg]]]",
		},
		{
			"delete middle leaf of three inserts results in a hole",
			1,
			[][]int64{{3}, {7}, {11}},
			4,
			8,
			`[0-16 [<del> 4-8:4]]`,
			`[0-16 [<ref> 0-4:1 (1b count=1) [leaf 1 msg]] [<ref> 8-12:3 (1b count=1) [leaf 1 msg]]]`,
		},
		{
			"delete root node (should delete links to all immediate descendants)",
			1,
			[][]int64{{4}},
			0,
			16,
			`[0-16
			  [<del> 0-4:2]
			  [<del> 4-8:2]
			  [<del> 8-12:2]
			  [<del> 12-16:2]]`,
			`[0-16]`,
		},
		{
			"delete leaf node in a depth-2 tree",
			2,
			[][]int64{{7}, {11}},
			4,
			8,
			`[0-64 [0-16:3 () [<del> 4-8:3]]]`,
			`[0-64 [0-16:3 (1b count=2) [<ref> 8-12:2 (1b count=1) [leaf 1 msg]]]]`,
		},
		{
			"delete depth-1 inner node in a depth-2 tree",
			2,
			[][]int64{{7}, {20}},
			0,
			16,
			`[0-64 [<del> 0-16:3]]`,
			`[0-64 [<ref> 16-32:2 (1b count=1) [<ref> 20-24:2 (1b count=1) [leaf 1 msg]]]]`,
		},
		{
			"delete that partially clears a populated leaf",
			1,
			[][]int64{{3}, {7}},
			0,
			3,
			`[0-16 [0-4:3 () [leaf 0 msgs]-<del 0 3>-> ??]]`,
			`[0-16 [0-4:3 (1b count=1) [leaf 0 msgs]-<del 0-3>->[leaf 1 msg]] [<ref> 4-8:2 (1b count=1) [leaf 1 msg]]]`,
		},
		{
			"delete that partially clears an unpopulated leaf - results in no new empty node",
			1,
			[][]int64{{3}, {7}},
			0,
			11e9,
			`[0-16 [<del> 0-4:3] [<del> 4-8:3] [<del> 8-12:3] [<del> 12-16:3]]`,
			`[0-16]`,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			// Setup: construct a partial tree with the given inserts
			setup := tree.MergeInserts(
				ctx, t, 0, util.Pow(uint64(4), int(c.height+1)), c.height, 4, c.prep,
			)

			// Construct a partial tree with the deletes
			tmpRoot := nodestore.NewInnerNode(c.height, 0, util.Pow(uint64(4), int(c.height+1)), 4)
			version := len(c.prep) + 1
			partial, err := tree.DeleteMessagesInRange(ctx, tmpRoot, uint64(version), c.start*1e9, c.end*1e9)
			require.NoError(t, err)

			repr, err := tree.Print(ctx, partial, setup)
			require.NoError(t, err)
			assertEqualTrees(t, c.partialResult, repr)

			// Merge the two trees, setup and partial
			final, err := tree.Merge(ctx, setup, partial)
			require.NoError(t, err)
			// Print the tree to compare with expected output
			repr, err = tree.Print(ctx, final, setup)
			require.NoError(t, err)
			assertEqualTrees(t, c.mergedResult, repr)
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
	s = strings.ReplaceAll(s, "  ", " ")
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
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "bytesUncompressed", int64(0)),
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
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "bytesUncompressed", int64(0)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "minObservedTime", int64(4097)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "maxObservedTime", int64(4097)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "bytesUncompressed", int64(0)),
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
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "bytesUncompressed", int64(0)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "minObservedTime", int64(262143)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "maxObservedTime", int64(262143)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "bytesUncompressed", int64(0)),
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
		inserts   [][]int64
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
			"[0-4096 [<ref> 0-64:1 (1b count=1) [leaf 1 msg]] [64-128:1 (1b count=1) [leaf 1 msg]]]",
		},
		{
			"merge into populated tree, overlapping",
			1,
			[][]int64{{33}, {120}},
			[][]int64{{100}},
			"[0-4096 [<ref> 0-64:1 (1b count=1) [leaf 1 msg]] [64-128:1 (1b count=2) [leaf 1 msg]->[leaf 1 msg]]]",
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
			`[0-262144 [0-4096:1 (1b count=3) [<ref> 0-64:1 (1b count=1) [leaf 1 msg]]
			[<ref> 64-128:2 (1b count=1) [leaf 1 msg]] [960-1024:1 (1b count=1) [leaf 1 msg]]]]`,
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			base := tree.MergeInserts(
				ctx, t, 0, util.Pow(uint64(64), int(c.height+1)), c.height, 64, c.prep,
			)
			partial := tree.MergeInserts(
				ctx, t, 0, util.Pow(uint64(64), int(c.height+1)), c.height, 64, c.inserts,
			)
			overlay, err := tree.Merge(ctx, base, partial)
			require.NoError(t, err)
			repr, err := tree.Print(ctx, overlay, base)
			require.NoError(t, err)
			assertEqualTrees(t, c.expected, repr)
		})
	}
}
