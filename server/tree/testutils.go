package tree

import (
	"bytes"
	"context"
	"io"
	"testing"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/nodestore"
	"golang.org/x/exp/maps"
)

func GetSchema(t *testing.T, r io.ReadSeeker) *fmcap.Schema {
	t.Helper()
	reader, err := mcap.NewReader(r)
	require.NoError(t, err)
	info, err := reader.Info()
	require.NoError(t, err)
	require.Len(t, info.Schemas, 1)
	return maps.Values(info.Schemas)[0]
}

// MergeInserts executes a list of inserts and then merges the resulting partial
// trees into a single tree. Times are expected in seconds. Each batch of times
// is expected to land on a single leaf.
func MergeInserts(
	ctx context.Context,
	t *testing.T,
	version uint64,
	start uint64,
	end uint64,
	height uint8,
	bfactor int,
	times [][]int64,
) (uint64, TreeReader) {
	t.Helper()
	trees := make([]TreeReader, 0, len(times)+1)

	// Add a base tree with the requested parameters
	root := nodestore.NewInnerNode(height, start, end, bfactor)
	mt := NewMemTree(nodestore.RandomNodeID(), root)
	data, err := mt.ToBytes(ctx, version)
	require.NoError(t, err)
	base := &MemTree{}
	require.NoError(t, base.FromBytes(ctx, data))
	trees = append(trees, base)

	for _, batch := range times {
		root := nodestore.NewInnerNode(height, start, end, bfactor)
		buf := &bytes.Buffer{}
		stats := mcap.WriteFile(t, buf, batch)
		tmp, err := NewInsert(ctx, root, version, uint64(batch[0]*1e9), nil, stats, buf.Bytes())
		require.NoError(t, err)

		repr, err := Print(ctx, tmp)
		require.NoError(t, err)
		t.Log(repr)

		trees = append(trees, tmp)
	}

	buf := &bytes.Buffer{}
	_, err = Merge(ctx, buf, version, nil, trees...)
	require.NoError(t, err)

	output := &MemTree{}
	require.NoError(t, output.FromBytes(ctx, buf.Bytes()))
	version++
	return version, output
}

func Validate(t *testing.T, ctx context.Context, tree TreeReader) {
	t.Helper()
	root := tree.Root()
	stack := []nodestore.NodeID{root}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node, err := tree.Get(ctx, id)
		require.NoError(t, err)

		switch n := node.(type) {
		case *nodestore.InnerNode:
			for _, child := range n.Children {
				if child == nil {
					continue
				}
				stack = append(stack, child.ID)
			}
		case *nodestore.LeafNode:
			_, reader, err := tree.GetLeafNode(ctx, id)
			require.NoError(t, err)

			data, err := io.ReadAll(reader)
			require.NoError(t, err)

			// well-formed mcap data
			r, err := mcap.NewReader(bytes.NewReader(data))
			require.NoError(t, err)
			_, err = r.Info()
			require.NoError(t, err)

			// no-op
		default:
			t.Fatalf("unexpected node type: %T", node)
		}
	}
}
