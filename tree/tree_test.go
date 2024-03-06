package tree_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
)

func TestNodeMerge(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		depth     uint8
		nodes     [][]uint64
		repr      string
	}{
		{
			"empty trees",
			1,
			[][]uint64{{}, {}},
			"[0-4096:0]",
		},
		{
			"same leaf bucket populated",
			1,
			[][]uint64{{10}, {12}},
			"[0-4096:2 [0-64:3 [leaf 697]]]",
		},
		{
			"new write conflicts one bucket and inserts one new one",
			1,
			[][]uint64{{10}, {12, 70}},
			"[0-4096:3 [0-64:4 [leaf 697]] [64-128:4 [leaf 566]]]",
		},
		{
			"no conflicts",
			1,
			[][]uint64{{10}, {70}},
			"[0-4096:2 [0-64:3 [leaf 566]] [64-128:3 [leaf 566]]]",
		},
		{
			"depth 2",
			2,
			[][]uint64{{10}, {12, 70}},
			"[0-262144:3 [0-4096:4 [0-64:4 [leaf 697]] [64-128:4 [leaf 566]]]]",
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ns := nodestore.MockNodestore(ctx, t)
			version := uint64(1)
			nodeIDs := make([]nodestore.NodeID, len(c.nodes))
			for i, node := range c.nodes {
				rootID, err := ns.NewRoot(ctx, 0, util.Pow(uint64(64), int(c.depth)+1), 64, 64)
				require.NoError(t, err)
				for _, time := range node {
					buf := &bytes.Buffer{}
					mcap.WriteFile(t, buf, []uint64{time})
					rootID, _, err = tree.Insert(ctx, ns, rootID, version, time*1e9, buf.Bytes())
					require.NoError(t, err)
					version++
				}
				nodeIDs[i] = rootID
			}
			merged, err := tree.NodeMerge(ctx, ns, version, nodeIDs)
			require.NoError(t, err)
			require.Positive(t, len(merged))

			repr, err := tree.PrintTree(ctx, ns, merged[0], version-1)
			require.NoError(t, err)
			assert.Equal(t, c.repr, repr)
		})
	}
}

func TestTreeInsert(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		depth     uint8
		times     []uint64
		repr      string
	}{
		{
			"empty tree",
			1,
			[]uint64{},
			"[0-4096:0]",
		},
		{
			"single insert",
			1,
			[]uint64{10},
			"[0-4096:1 [0-64:1 [leaf 566]]]",
		},
		{
			"two inserts same bucket get merged",
			1,
			[]uint64{10, 20},
			"[0-4096:2 [0-64:2 [leaf 697]]]",
		},
		{
			"inserts in different bucket, simulate single inserts",
			1,
			[]uint64{10, 20, 128, 256},
			"[0-4096:4 [0-64:2 [leaf 697]] [128-192:3 [leaf 568]] [256-320:4 [leaf 568]]]",
		},
		{
			"depth 2",
			2,
			[]uint64{10, 20, 4097},
			"[0-262144:3 [0-4096:2 [0-64:2 [leaf 697]]] [4096-8192:3 [4096-4160:3 [leaf 568]]]]",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ns := nodestore.MockNodestore(ctx, t)
			rootID, err := ns.NewRoot(ctx, 0, util.Pow(uint64(64), int(c.depth)+1), 64, 64)
			require.NoError(t, err)
			version := uint64(1)
			var path []nodestore.NodeID
			for _, time := range c.times {
				buf := &bytes.Buffer{}
				mcap.WriteFile(t, buf, []uint64{time})
				_, path, err = tree.Insert(ctx, ns, rootID, version, time*1e9, buf.Bytes())
				require.NoError(t, err)
				rootID, err = ns.Flush(ctx, path...)
				require.NoError(t, err)
				version++
			}
			repr, err := tree.PrintTree(ctx, ns, rootID, version-1)
			require.NoError(t, err)
			assert.Equal(t, c.repr, repr)
		})
	}
}
