package tree_test

import (
	"bytes"
	"context"
	"testing"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
)

func TestTreeErrors(t *testing.T) {
	ctx := context.Background()
	ns := nodestore.MockNodestore(ctx, t)
	t.Run("inserting into a non-existent root", func(t *testing.T) {
		id := nodestore.RandomNodeID()
		_, _, err := tree.Insert(ctx, ns, id, 0, 0, []byte{})
		require.ErrorIs(t, err, nodestore.NodeNotFoundError{NodeID: id})
	})
	t.Run("inserting into a node with mislinked children", func(t *testing.T) {
		id1 := nodestore.RandomNodeID()
		node1 := nodestore.NewInnerNode(5, 0, 1e9, 64)
		id2 := nodestore.RandomNodeID()
		node1.Children[0] = &nodestore.Child{
			ID:      id2,
			Version: 2,
		}
		require.NoError(t, ns.StageWithID(id1, node1))
		rootID, err := ns.Flush(ctx, id1)
		require.NoError(t, err)
		_, _, err = tree.Insert(ctx, ns, rootID, 10, 3, []byte{})
		require.ErrorIs(t, err, nodestore.NodeNotFoundError{NodeID: id2})
	})
	t.Run("inserting into a mislinked leaf node", func(t *testing.T) {
		id1 := nodestore.RandomNodeID()
		node1 := nodestore.NewInnerNode(1, 0, 1e9, 64)
		id2 := nodestore.RandomNodeID()
		node1.Children[0] = &nodestore.Child{
			ID:      id2,
			Version: 2,
		}
		require.NoError(t, ns.StageWithID(id1, node1))
		rootID, err := ns.Flush(ctx, id1)
		require.NoError(t, err)
		_, _, err = tree.Insert(ctx, ns, rootID, 10, 3, []byte{})
		require.ErrorIs(t, err, nodestore.NodeNotFoundError{NodeID: id2})
	})
	t.Run("inserting into a leaf node that should be an inner node", func(t *testing.T) {
		id1 := nodestore.RandomNodeID()
		node1 := nodestore.NewInnerNode(2, 0, 1e9, 64)
		id2 := nodestore.RandomNodeID()
		node2 := nodestore.NewLeafNode([]byte{})
		node1.Children[0] = &nodestore.Child{
			ID:      id2,
			Version: 2,
		}
		require.NoError(t, ns.StageWithID(id1, node1))
		require.NoError(t, ns.StageWithID(id2, node2))
		rootID, err := ns.Flush(ctx, id1, id2)
		require.NoError(t, err)
		_, _, err = tree.Insert(ctx, ns, rootID, 10, 3, []byte{})
		require.ErrorIs(t, err, tree.UnexpectedNodeError{})
	})
	t.Run("inserting into an inner node that should be a leaf node", func(t *testing.T) {
		id1 := nodestore.RandomNodeID()
		node1 := nodestore.NewInnerNode(1, 0, 1e9, 64)
		id2 := nodestore.RandomNodeID()
		node2 := nodestore.NewInnerNode(1, 0, 1e9, 64)
		node1.Children[0] = &nodestore.Child{
			ID:      id2,
			Version: 2,
		}
		require.NoError(t, ns.StageWithID(id1, node1))
		require.NoError(t, ns.StageWithID(id2, node2))
		rootID, err := ns.Flush(ctx, id1, id2)
		require.NoError(t, err)
		_, _, err = tree.Insert(ctx, ns, rootID, 10, 3, []byte{})
		require.ErrorIs(t, err, tree.UnexpectedNodeError{})
	})
	t.Run("merging corrupt data into a leaf", func(t *testing.T) {
		rootID, err := ns.NewRoot(ctx, 0, 1e9, 64, 64)
		require.NoError(t, err)
		buf := &bytes.Buffer{}
		mcap.WriteFile(t, buf, []uint64{10})
		rootID, _, err = tree.Insert(ctx, ns, rootID, 10, 3, buf.Bytes())
		require.NoError(t, err)
		_, _, err = tree.Insert(ctx, ns, rootID, 10, 3, []byte{1, 2, 3})
		require.ErrorIs(t, err, &fmcap.ErrBadMagic{})
	})
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
			"[0-4096:1 [0-64:1 [leaf 1 msg]]]",
		},
		{
			"two inserts same bucket get merged",
			1,
			[]uint64{10, 20},
			"[0-4096:2 [0-64:2 [leaf 2 msgs]]]",
		},
		{
			"inserts in different bucket, simulate single inserts",
			1,
			[]uint64{10, 20, 128, 256},
			"[0-4096:4 [0-64:2 [leaf 2 msgs]] [128-192:3 [leaf 1 msg]] [256-320:4 [leaf 1 msg]]]",
		},
		{
			"depth 2",
			2,
			[]uint64{10, 20, 4097},
			"[0-262144:3 [0-4096:2 [0-64:2 [leaf 2 msgs]]] [4096-8192:3 [4096-4160:3 [leaf 1 msg]]]]",
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
