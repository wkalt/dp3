package tree_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
)

func TestTreeInsertDataNode(t *testing.T) {
	cases := []struct {
		assertion string
		depth     int
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
			"[0-4096:1 [0-64:1 [data *]]]",
		},
		{
			"two inserts same bucket get merged",
			1,
			[]uint64{10, 20},
			"[0-4096:2 [0-64:2 [data **]]]",
		},
		{
			"inserts in different bucket, simulate single inserts",
			1,
			[]uint64{10, 20, 128, 256},
			"[0-4096:4 [0-64:2 [data **]] [128-192:3 [data *]] [256-320:4 [data *]]]",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			store := storage.NewMemStore()
			cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
			ns := nodestore.NewNodestore(store, cache)
			tr, err := tree.NewTree(0, util.Pow(uint64(64), c.depth+1), 64, 64, ns)
			require.NoError(t, err)
			for _, time := range c.times {
				require.NoError(t, tr.Insert(time, []byte{'a'}))
			}
			assert.Equal(t, c.repr, tr.String())
		})
	}
}
