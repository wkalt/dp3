package tree_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/nodestore"
	"github.com/wkalt/dp3/server/tree"
	"github.com/wkalt/dp3/server/util"
)

func TestTreeErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("root is nil", func(t *testing.T) {
		_, err := tree.NewInsert(ctx, nil, 0, 0, nil, nil, []byte{0x01})
		require.Error(t, err)
	})

	t.Run("out of bounds insert", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		_, err := tree.NewInsert(ctx, root, 0, 10000*1e9, nil, nil, []byte{0x01})
		require.ErrorIs(t, err, tree.OutOfBoundsError{})
	})

	t.Run("out of bounds delete", func(t *testing.T) {
		rootStart, rootEnd := uint64(0), uint64(4096)
		start, end := uint64(10000*1e9), uint64(10001*1e9)
		root := nodestore.NewInnerNode(1, rootStart, rootEnd, 64)
		_, err := tree.NewDelete(ctx, root, 0, start, end)
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
		_, err := tree.NewDelete(ctx, root, 0, start, end)
		require.EqualError(t, err, fmt.Sprintf("invalid range: start %d cannot be >= end %d", start, end))
	})
}

func TestMergeErrors(t *testing.T) {
	ctx := context.Background()
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
		tw2, err := tree.NewInsert(ctx, root2, 0, 63*1e9, nil, nil, []byte{0x01})
		require.NoError(t, err)

		root3 := nodestore.NewInnerNode(2, 0, 64*64*64, 64)
		tw3 := tree.NewMemTree(nodestore.RandomNodeID(), root3)

		buf := &bytes.Buffer{}
		_, err = tree.Merge(ctx, buf, 1, tw3, tw, tw2)
		require.Error(t, err)
	})

	t.Run("root does not exist", func(t *testing.T) {
		root := nodestore.NewInnerNode(1, 0, 4096, 64)
		tw, err := tree.NewInsert(ctx, root, 0, 1000*1e9, nil, nil, []byte{0x01})
		require.NoError(t, err)
		tw.SetRoot(nodestore.RandomNodeID())

		root2 := nodestore.NewInnerNode(2, 0, 4096, 64)
		tw2 := tree.NewMemTree(nodestore.RandomNodeID(), root2)

		buf := &bytes.Buffer{}
		_, err = tree.Merge(ctx, buf, 1, tw2, tw)
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
			"[0-4096 [0-64:1 (1b count=2) [leaf 2 msgs]]]",
		},
		{
			"inserts into different buckets",
			1,
			[][]int64{{10}, {100}, {256}, {1000}},
			`[0-4096 [0-64:1 (1b count=1) [leaf 1 msg]] [64-128:1 (1b count=1) [leaf 1 msg]]
			[256-320:1 (1b count=1) [leaf 1 msg]] [960-1024:1 (1b count=1) [leaf 1 msg]]]`,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			_, output := tree.MergeInserts(
				ctx, t, 1, 0, util.Pow(uint64(64), int(c.height+1)),
				c.height, 64, c.times,
			)
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
			"[0-16 [<del> 0-4:2] [<del> 4-8:2]]",
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
			"[0-16 [<del> 0-4:2]]",
			"[0-16 [<ref> 4-8:1 (1b count=1) [leaf 1 msg]]]",
		},
		{
			"delete middle leaf of three inserts results in a hole",
			1,
			[][]int64{{3}, {7}, {11}},
			4,
			8,
			`[0-16 [<del> 4-8:2]]`,
			`[0-16 [<ref> 0-4:1 (1b count=1) [leaf 1 msg]] [<ref> 8-12:1 (1b count=1) [leaf 1 msg]]]`,
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
			"delete that partially clears a populated leaf",
			1,
			[][]int64{{3}, {7}},
			0,
			3,
			`[0-16 [0-4:2 () [leaf 0 msgs]-<del 0 3>-> ??]]`,
			`[0-16 [0-4:3 (1b count=1) [leaf 0 msgs]-<del 0-3>->[leaf 1 msg]] [<ref> 4-8:1 (1b count=1) [leaf 1 msg]]]`,
		},
		{
			"delete that partially clears an unpopulated leaf - results in no new empty node",
			1,
			[][]int64{{3}, {7}},
			0,
			11e9,
			`[0-16 [<del> 0-4:2] [<del> 4-8:2] [<del> 8-12:2] [<del> 12-16:2]]`,
			`[0-16]`,
		},
		{
			"delete depth-1 inner node in a depth-2 tree",
			2,
			[][]int64{{7}, {20}},
			0,
			16,
			`[0-64 [<del> 0-16:2]]`,
			`[0-64 [<ref> 16-32:1 (1b count=1) [<ref> 20-24:1 (1b count=1) [leaf 1 msg]]]]`,
		},
		{
			"delete leaf node in a depth-2 tree",
			2,
			[][]int64{{7}, {11}},
			4,
			8,
			`[0-64 [0-16:2 () [<del> 4-8:2]]]`,
			`[0-64 [0-16:3 (1b count=2) [<ref> 8-12:1 (1b count=1) [leaf 1 msg]]]]`,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			// Setup: construct a base tree with the given inserts
			version, base := tree.MergeInserts(
				ctx, t, 1, 0, util.Pow(uint64(4), int(c.height+1)), c.height, 4, c.prep,
			)

			// Construct a partial tree with the deletes
			tmpRoot := nodestore.NewInnerNode(c.height, 0, util.Pow(uint64(4), int(c.height+1)), 4)
			partial, err := tree.NewDelete(ctx, tmpRoot, version, c.start*1e9, c.end*1e9)
			require.NoError(t, err)

			version++

			repr, err := tree.Print(ctx, partial, base)
			require.NoError(t, err)
			assertEqualTrees(t, c.partialResult, repr)

			// Merge the two trees, setup and partial
			buf := &bytes.Buffer{}
			_, err = tree.Merge(ctx, buf, version, base, partial)
			require.NoError(t, err)

			overlay := &tree.MemTree{}
			require.NoError(t, overlay.FromBytes(ctx, buf.Bytes()))
			// Print the tree to compare with expected output
			repr, err = tree.Print(ctx, overlay, base)
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

// nolint: dupl
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
			"single insert",
			[][]int64{{100}},
			0,
			100e9,
			600e9,
			[]nodestore.StatRange{
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Text, "data", "min", "hello"),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Text, "data", "max", "hello"),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "mean", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "min", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "max", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "sum", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "count", float64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "bytesUncompressed", int64(11)),
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
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Text, "data", "min", "hello"),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Text, "data", "max", "hello"),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Float, "count", "mean", float64(2024)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Float, "count", "min", float64(2024)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Float, "count", "max", float64(2024)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Float, "count", "sum", float64(2024)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Float, "count", "count", float64(1)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "bytesUncompressed", int64(11)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "minObservedTime", int64(4097)),
				nodestore.NewStatRange(testhash, 4096e9, 4160e9, nodestore.Int, "", "maxObservedTime", int64(4097)),

				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Text, "data", "min", "hello"),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Text, "data", "max", "hello"),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "mean", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "min", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "max", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "sum", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "count", float64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "bytesUncompressed", int64(11)),
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
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Text, "data", "min", "hello"),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Text, "data", "max", "hello"),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Float, "count", "mean", float64(2024)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Float, "count", "min", float64(2024)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Float, "count", "max", float64(2024)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Float, "count", "sum", float64(2024)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Float, "count", "count", float64(1)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "bytesUncompressed", int64(11)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "minObservedTime", int64(262143)),
				nodestore.NewStatRange(testhash, 262080e9, 262144e9, nodestore.Int, "", "maxObservedTime", int64(262143)),

				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Text, "data", "min", "hello"),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Text, "data", "max", "hello"),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "mean", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "min", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "max", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "sum", float64(2024)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Float, "count", "count", float64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "messageCount", int64(1)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "bytesUncompressed", int64(11)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "minObservedTime", int64(100)),
				nodestore.NewStatRange(testhash, 64e9, 128e9, nodestore.Int, "", "maxObservedTime", int64(100)),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			_, tr := tree.MergeInserts(ctx, t, 0, 0, 64*64*64, 2, 64, c.messages)
			statrange, err := tree.GetStatRange(ctx, tr, c.start, c.end, c.granularity)
			require.NoError(t, err)
			for _, sr := range c.expected {
				require.Contains(t, statrange, sr)
			}
		})
	}
}

func TestMergingOutOfVersionOrder(t *testing.T) {
	ctx := context.Background()

	buf1 := &bytes.Buffer{}
	mcap.WriteFile(t, buf1, []int64{100})
	buf2 := &bytes.Buffer{}
	mcap.WriteFile(t, buf2, []int64{100})
	buf3 := &bytes.Buffer{}
	mcap.WriteFile(t, buf3, []int64{100})

	root1 := nodestore.NewInnerNode(1, 0, 4096, 64)
	tw1, err := tree.NewInsert(ctx, root1, 1, 100, nil, nil, buf1.Bytes())
	require.NoError(t, err)

	root2 := nodestore.NewInnerNode(1, 0, 4096, 64)
	tw2, err := tree.NewInsert(ctx, root2, 2, 100, nil, nil, buf2.Bytes())
	require.NoError(t, err)

	root3 := nodestore.NewInnerNode(1, 0, 4096, 64)
	tw3, err := tree.NewInsert(ctx, root3, 3, 100, nil, nil, buf3.Bytes())
	require.NoError(t, err)

	dest1 := tree.NewMemTree(nodestore.RandomNodeID(), root1)

	buf := &bytes.Buffer{}
	_, err = tree.Merge(ctx, buf, 3, dest1, tw2, tw3, tw1)
	require.NoError(t, err)
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
			"[0-4096 [64-128:3 (1b count=1) [leaf 1 msg]]]",
		},
		{
			"merge multiple height-1 inner nodes into empty tree",
			1,
			[][]int64{},
			[][]int64{{33}, {120}},
			"[0-4096 [0-64:3 (1b count=1) [leaf 1 msg]] [64-128:3 (1b count=1) [leaf 1 msg]]]",
		},
		{
			"merge into populated tree, nonoverlapping",
			1,
			[][]int64{{33}},
			[][]int64{{100}},
			"[0-4096 [<ref> 0-64:1 (1b count=1) [leaf 1 msg]] [64-128:3 (1b count=1) [leaf 1 msg]]]",
		},
		{
			"merge into populated tree, overlapping",
			1,
			[][]int64{{33}, {120}},
			[][]int64{{100}},
			"[0-4096 [<ref> 0-64:1 (1b count=1) [leaf 1 msg]] [64-128:3 (1b count=2) [leaf 1 msg]->[leaf 1 msg]]]",
		},
		{
			"merge into a populated tree, multiple overlapping",
			1,
			[][]int64{{33}, {120}},
			[][]int64{{39}, {121}},
			"[0-4096 [0-64:3 (1b count=2) [leaf 1 msg]->[leaf 1 msg]] [64-128:3 (1b count=2) [leaf 1 msg]->[leaf 1 msg]]]",
		},
		{
			"depth 2",
			2,
			[][]int64{{33}, {120}},
			[][]int64{{1000}},
			`[0-262144 [0-4096:3 (1b count=3) [<ref> 0-64:1 (1b count=1) [leaf 1 msg]]
			[<ref> 64-128:1 (1b count=1) [leaf 1 msg]] [960-1024:3 (1b count=1) [leaf 1 msg]]]]`,
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			_, base := tree.MergeInserts(
				ctx, t, 1, 0, util.Pow(uint64(64), int(c.height+1)), c.height, 64, c.prep,
			)
			_, partial := tree.MergeInserts(
				ctx, t, 2, 0, util.Pow(uint64(64), int(c.height+1)), c.height, 64, c.inserts,
			)
			buf := &bytes.Buffer{}
			_, err := tree.Merge(ctx, buf, 3, base, partial)
			require.NoError(t, err)

			overlay := &tree.MemTree{}
			require.NoError(t, overlay.FromBytes(ctx, buf.Bytes()))

			repr, err := tree.Print(ctx, overlay, base)
			require.NoError(t, err)
			assertEqualTrees(t, c.expected, repr)
		})
	}
}
