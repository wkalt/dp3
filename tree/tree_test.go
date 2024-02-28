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
			cache := util.NewLRU[nodestore.Node](1e6)
			ns := nodestore.NewNodestore(store, cache)
			tr, err := tree.NewTree(0, util.Pow(uint64(64), c.depth+1), 64, 64, ns)
			require.NoError(t, err)
			for _, time := range c.times {
				require.NoError(t, tr.InsertDataNode(time, []byte{'a'}))
			}
			assert.Equal(t, c.repr, tr.String())
		})
	}
}

func TestTreeInsert(t *testing.T) {
	cases := []struct {
		assertion string
		depth     int
		times     [][]uint64
		repr      string
	}{
		{
			"empty tree",
			1,
			[][]uint64{},
			"[0-4096:0]",
		},
		{
			"single insert",
			1,
			[][]uint64{{10}},
			"[0-4096:1 [0-64:1 [leaf 10]]]",
		},
		{
			"two inserts same bucket",
			1,
			[][]uint64{{10}, {20}},
			"[0-4096:2 [0-64:2 [leaf 10 20]]]",
		},
		{
			"inserts in different bucket, simulate single inserts",
			1,
			[][]uint64{{10}, {20}, {128}, {256}},
			"[0-4096:4 [0-64:2 [leaf 10 20]] [128-192:3 [leaf 128]] [256-320:4 [leaf 256]]]",
		},
		{
			"batch insert",
			1,
			[][]uint64{{10, 20, 30}},
			"[0-4096:1 [0-64:1 [leaf 10 20 30]]]",
		},
		{
			"batch insert crossing buckets",
			1,
			[][]uint64{{10, 127, 128, 129}},
			"[0-4096:1 [0-64:1 [leaf 10]] [64-128:1 [leaf 127]] [128-192:1 [leaf 128 129]]]",
		},
		{
			"two batches partly intersecting",
			1,
			[][]uint64{
				{10, 127, 128, 129},
				{67},
			},
			"[0-4096:2 [0-64:1 [leaf 10]] [64-128:2 [leaf 67 127]] [128-192:1 [leaf 128 129]]]",
		},
		{
			"time sorted order maintained",
			1,
			[][]uint64{{3, 2, 1}},
			"[0-4096:1 [0-64:1 [leaf 1 2 3]]]",
		},
		{
			"depth 2 tree",
			2,
			[][]uint64{{10}},
			"[0-262144:1 [0-4096:1 [0-64:1 [leaf 10]]]]",
		},
		{
			"depth 2 tree, two buckets",
			2,
			[][]uint64{{10, 67}},
			"[0-262144:1 [0-4096:1 [0-64:1 [leaf 10]] [64-128:1 [leaf 67]]]]",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			store := storage.NewMemStore()
			cache := util.NewLRU[nodestore.Node](1e6)
			ns := nodestore.NewNodestore(store, cache)
			tr, err := tree.NewTree(0, util.Pow(uint64(64), c.depth+1), 64, 64, ns)
			require.NoError(t, err)
			for _, time := range c.times {
				records := make([]nodestore.Record, len(time))
				for i, t := range time {
					records[i] = nodestore.NewRecord(t, []byte{})
				}
				require.NoError(t, tr.Insert(records))
			}
			assert.Equal(t, c.repr, tr.String())
		})
	}
}
