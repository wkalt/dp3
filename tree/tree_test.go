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

func TestTreeErrors(t *testing.T) {
	ctx := context.Background()
	ns := nodestore.MockNodestore(ctx, t)
	t.Run("inserting into a non-existent root", func(t *testing.T) {
		id := nodestore.RandomNodeID()
		_, _, err := tree.Insert(ctx, ns, id, 0, 0, []byte{})
		require.ErrorIs(t, err, nodestore.NodeNotFoundError{NodeID: id})
	})
	t.Run("out of bounds insert", func(t *testing.T) {
		rootID, err := ns.NewRoot(ctx, 100, 1e9, 64, 64)
		require.NoError(t, err)
		_, _, err = tree.Insert(ctx, ns, rootID, 10, 1e9, []byte{})
		require.ErrorIs(t, err, tree.OutOfBoundsError{})
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
		rootID, err := ns.Flush(ctx, 2, id1)
		require.NoError(t, err)
		_, _, err = tree.Insert(ctx, ns, rootID, 10, 3, []byte{})
		require.ErrorIs(t, err, nodestore.NodeNotFoundError{NodeID: id2})
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
			"[0-4096:4 [0-64:4 [leaf 2 msgs]] [128-192:4 [leaf 1 msg]] [256-320:4 [leaf 1 msg]]]",
		},
		{
			"depth 2",
			2,
			[]uint64{10, 20, 4097},
			"[0-262144:3 [0-4096:3 [0-64:3 [leaf 2 msgs]]] [4096-8192:3 [4096-4160:3 [leaf 1 msg]]]]",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ns := nodestore.MockNodestore(ctx, t)
			rootID, err := ns.NewRoot(ctx, 0, util.Pow(uint64(64), int(c.depth)+1), 64, 64)
			require.NoError(t, err)
			version := uint64(0)
			roots := make([]nodestore.NodeID, len(c.times))
			for i, time := range c.times {
				buf := &bytes.Buffer{}
				mcap.WriteFile(t, buf, []uint64{time})
				version++
				rootID, path, err := tree.Insert(ctx, ns, rootID, version, time*1e9, buf.Bytes())
				require.NoError(t, err)

				require.NoError(t, ns.WALFlush(ctx, "producer", "topic", version, path))
				require.NoError(t, err)
				roots[i] = rootID
			}

			rootID, err = ns.WALMerge(ctx, rootID, version, roots)
			require.NoError(t, err)
			repr, err := ns.Print(ctx, rootID, version)
			require.NoError(t, err)
			assert.Equal(t, c.repr, repr)
		})
	}
}

func TestRootConstruction(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		start     string
		end       string
		width     int
		bfactor   int
		time      string
		repr      string
	}{
		{
			"epoch to 2050 with 60 second buckets",
			"1970-01-01",
			"2030-01-01",
			60,
			64,
			"1970-01-01",
			`[0-64424509440:1 [0-1006632960:1 [0-15728640:1 [0-245760:1 [0-3840:1 [0-60:1 [leaf 1 msg]]]]]]]`,
		},
		{
			"single year 60 second buckets",
			"1970-01-01",
			"1971-01-01",
			60,
			64,
			"1970-01-02",
			`[0-1006632960:1 [0-15728640:1 [0-245760:1 [84480-88320:1 [86400-86460:1 [leaf 1 msg]]]]]]`,
		},
		{
			"single year 60 second buckets bfactor 20",
			"1970-01-01",
			"1971-01-01",
			60,
			20,
			"1970-01-02",
			`[0-192000000:1 [0-9600000:1 [0-480000:1 [72000-96000:1 [86400-87600:1 [86400-86460:1 [leaf 1 msg]]]]]]]`,
		},
		{
			"single year full day buckets bfactor 365",
			"1970-01-01",
			"1971-01-01",
			60 * 60 * 24,
			365,
			"1970-01-02",
			`[0-31536000:1 [86400-172800:1 [leaf 1 msg]]]`,
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ns := nodestore.MockNodestore(ctx, t)
			rootID, err := ns.NewRoot(
				ctx,
				util.DateSeconds(c.start),
				util.DateSeconds(c.end),
				c.width,
				c.bfactor,
			)
			require.NoError(t, err)

			version := uint64(1)

			buf := &bytes.Buffer{}
			mcap.WriteFile(t, buf, []uint64{10})
			timestamp := util.DateSeconds(c.time) * 1e9
			_, path, err := tree.Insert(ctx, ns, rootID, version, timestamp, buf.Bytes())
			require.NoError(t, err)
			rootID, err = ns.Flush(ctx, version, path...)
			require.NoError(t, err)

			repr, err := ns.Print(ctx, rootID, version)
			require.NoError(t, err)
			assert.Equal(t, c.repr, repr)
		})
	}
}
