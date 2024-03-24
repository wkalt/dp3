package tree_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
)

func TestMemtreeSerialization(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion  string
		height     uint8
		timestamps []uint64
		expected   string
	}{
		{
			"two trees",
			1,
			[]uint64{100, 120},
			"[0-4096 [0-64:1 (count=2) [leaf 2 msgs]]]",
		},
		{
			"tree trees",
			1,
			[]uint64{100, 120, 1024 * 1e9},
			"[0-4096 [0-64:1 (count=2) [leaf 2 msgs]] [1024-1088:2 (count=1) [leaf 1 msg]]]",
		},
		{
			"height 2",
			2,
			[]uint64{100, 120, 1024 * 1e9},
			"[0-262144 [0-4096:2 (count=3) [0-64:1 (count=2) [leaf 2 msgs]] [1024-1088:2 (count=1) [leaf 1 msg]]]]",
		},
	}

	for _, c := range cases {
		root := nodestore.NewInnerNode(
			c.height,
			0,
			util.Pow(uint64(64), int(c.height+1)),
			64,
		)
		version := uint64(0)

		trees := make([]tree.TreeReader, len(c.timestamps))
		for i, ts := range c.timestamps {
			data := &bytes.Buffer{}
			id := nodestore.RandomNodeID()
			mt := tree.NewMemTree(id, root)
			mcap.WriteFile(t, data, []uint64{ts})
			require.NoError(t, tree.Insert(ctx, mt, version, ts, data.Bytes(), &nodestore.Statistics{
				MessageCount: 1,
			}))

			version++
			trees[i] = mt
		}

		var merged tree.MemTree
		require.NoError(t, tree.Merge(ctx, &merged, trees...))

		s1, err := tree.Print(ctx, &merged)
		require.NoError(t, err)

		bytes, err := merged.ToBytes(ctx, version)
		require.NoError(t, err)

		var merged2 tree.MemTree
		require.NoError(t, merged2.FromBytes(ctx, bytes))

		s2, err := tree.Print(ctx, &merged2)
		require.NoError(t, err)

		require.Equal(t, c.expected, s1, s2)
	}
}

func TestMemtreeMerge(t *testing.T) {
	cases := []struct {
		assertion string
		height    uint8
		inputs    [][]uint64
		expected  string
	}{
		{
			"one tree",
			2,
			[][]uint64{{100}},
			"[0-262144 [0-4096:1 (count=1) [0-64:1 (count=1) [leaf 1 msg]]]]",
		},
		{
			"two trees, overlapping",
			2,
			[][]uint64{{100e9}, {65e9}},
			"[0-262144 [0-4096:2 (count=2) [64-128:2 (count=2) [leaf 2 msgs]]]]",
		},
		{
			"two trees, disjoint",
			1,
			[][]uint64{{100e9}, {1000e9}},
			"[0-4096 [64-128:1 (count=1) [leaf 1 msg]] [960-1024:2 (count=1) [leaf 1 msg]]]",
		},
	}
	ctx := context.Background()
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			version := uint64(1)
			trees := []tree.TreeReader{}
			for _, input := range c.inputs {
				node := nodestore.NewInnerNode(c.height, 0, uint64(util.Pow(64, int(c.height)+1)), 64)
				pt := tree.NewMemTree(nodestore.RandomNodeID(), node)
				data := &bytes.Buffer{}
				mcap.WriteFile(t, data, input)
				require.NoError(t, tree.Insert(ctx, pt, version, input[0], data.Bytes(), &nodestore.Statistics{MessageCount: 1}))
				version++
				trees = append(trees, pt)
			}

			outputNode := nodestore.NewInnerNode(2, 0, uint64(util.Pow(64, int(c.height)+1)), 64)
			output := tree.NewMemTree(nodestore.RandomNodeID(), outputNode)
			require.NoError(t, tree.Merge(ctx, output, trees...))

			require.Equal(t, c.expected, output.String(ctx))
		})
	}
}
