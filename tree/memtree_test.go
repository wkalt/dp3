package tree_test

import (
	"bytes"
	"context"
	"io"
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
		timestamps []int64
		expected   string
	}{
		{
			"two trees",
			1,
			[]int64{100, 120},
			"[0-4096 [0-64:1 (1b count=2) [leaf 2 msgs]]]",
		},
		{
			"three trees",
			1,
			[]int64{100, 120, 1024 * 1e9},
			"[0-4096 [0-64:1 (1b count=2) [leaf 2 msgs]] [1024-1088:1 (1b count=1) [leaf 1 msg]]]",
		},
		{
			"height 2",
			2,
			[]int64{100, 120, 1024 * 1e9},
			"[0-262144 [0-4096:1 (1b count=3) [0-64:1 (1b count=2) [leaf 2 msgs]] [1024-1088:1 (1b count=1) [leaf 1 msg]]]]",
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
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
				stats := mcap.WriteFile(t, data, []int64{ts})
				mt, err := tree.NewInsert(ctx, root, version, uint64(ts), nil, stats, data.Bytes())
				require.NoError(t, err)
				version++
				trees[i] = mt
			}

			rootnode := nodestore.NewInnerNode(c.height, 0, util.Pow(uint64(64), int(c.height+1)), 64)
			dest := tree.NewMemTree(nodestore.RandomNodeID(), rootnode)

			buf := &bytes.Buffer{}
			_, err := tree.Merge(ctx, buf, 1, dest, trees...)
			require.NoError(t, err)

			merged := &tree.MemTree{}
			require.NoError(t, merged.FromBytes(ctx, buf.Bytes()))

			s1, err := tree.Print(ctx, merged)
			require.NoError(t, err)

			data, err := merged.ToBytes(ctx, version)
			require.NoError(t, err)

			var merged2 tree.MemTree
			require.NoError(t, merged2.FromBytes(ctx, data))

			s2, err := tree.Print(ctx, &merged2)
			require.NoError(t, err)

			require.Equal(t, c.expected, s1, s2)

			filetree, err := tree.NewFileTree(func() (io.ReadSeekCloser, error) {
				return util.NewReadSeekNopCloser(bytes.NewReader(data)), nil
			}, 0, len(data))
			require.NoError(t, err)

			tree.Validate(t, ctx, filetree)
		})
	}
}
