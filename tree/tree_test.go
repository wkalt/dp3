package tree_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/storage"
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
			"[0-4096:1 [0-64:1 [leaf 542]]]",
		},
		{
			"two inserts same bucket get merged",
			1,
			[]uint64{10, 20},
			"[0-4096:2 [0-64:2 [leaf 634]]]",
		},
		{
			"inserts in different bucket, simulate single inserts",
			1,
			[]uint64{10, 20, 128, 256},
			"[0-4096:4 [0-64:2 [leaf 634]] [128-192:3 [leaf 542]] [256-320:4 [leaf 539]]]",
		},
		{
			"depth 2",
			2,
			[]uint64{10, 20, 4097},
			"[0-262144:3 [0-4096:2 [0-64:2 [leaf 634]]] [4096-8192:3 [4096-4160:3 [leaf 542]]]]",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			store := storage.NewMemStore()
			cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
			ns := nodestore.NewNodestore(store, cache)
			rootID, err := ns.NewRoot(ctx, 0, util.Pow(uint64(64), int(c.depth)+1), 64, 64)
			require.NoError(t, err)
			version := uint64(1)
			for _, time := range c.times {
				buf := &bytes.Buffer{}
				mcap.WriteFile(t, buf, []uint64{time})
				rootID, _, err = tree.Insert(ctx, ns, rootID, version, time*1e9, buf.Bytes())
				require.NoError(t, err)
				version++
			}
			repr, err := tree.PrintTree(ctx, ns, rootID, version-1)
			require.NoError(t, err)
			assert.Equal(t, c.repr, repr)
		})
	}
}
