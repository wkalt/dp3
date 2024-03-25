package tree

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
)

// MergeInserts executes a list of inserts and then merges the resulting partial
// trees into a single tree.
func MergeInserts(
	ctx context.Context,
	t *testing.T,
	start uint64,
	end uint64,
	height uint8,
	bfactor int,
	times [][]uint64,
) TreeReader {
	t.Helper()
	version := uint64(1)
	trees := make([]TreeReader, len(times))
	for i, batch := range times {
		root := nodestore.NewInnerNode(height, start, end, bfactor)
		id := nodestore.RandomNodeID()
		tmp := NewMemTree(id, root)
		buf := &bytes.Buffer{}
		mcap.WriteFile(t, buf, batch)
		require.NoError(t, Insert(
			ctx, tmp, version, batch[0]*1e9, buf.Bytes(), &nodestore.Statistics{
				MessageCount: uint64(len(batch)),
			},
		))
		version++
		trees[i] = tmp
	}
	root := nodestore.NewInnerNode(height, start, end, bfactor)
	output := NewMemTree(nodestore.RandomNodeID(), root)
	if len(times) == 0 {
		return output
	}
	if len(times) == 1 {
		return trees[0]
	}

	root2 := nodestore.NewInnerNode(height, start, end, bfactor)
	base := NewMemTree(nodestore.RandomNodeID(), root2)

	require.NoError(t, Merge(ctx, output, base, trees...))

	// output is now the merged tree.
	return NewOverlay([]TreeReader{output, base})
}
