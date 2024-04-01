package tree

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
)

// MergeInserts executes a list of inserts and then merges the resulting partial
// trees into a single tree. Times are expected in seconds.
func MergeInserts(
	ctx context.Context,
	t *testing.T,
	start uint64,
	end uint64,
	height uint8,
	bfactor int,
	times [][]int64,
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
			ctx, tmp, version, uint64(batch[0]*1e9), buf.Bytes(), &nodestore.Statistics{
				MessageCount:    int64(len(batch)),
				ByteCount:       0,
				MaxObservedTime: util.Reduce(util.Max, 0, batch),
				MinObservedTime: util.Reduce(util.Min, math.MaxInt64, batch),
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
	require.NoError(t, Merge(ctx, output, output, trees...))
	return output
}
