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
			repr, err := tree.Print(ctx, ns, rootID, version-1)
			require.NoError(t, err)
			assert.Equal(t, c.repr, repr)
		})
	}
}
