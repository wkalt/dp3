package tree

import (
	"bytes"
	"context"
	"io"
	"math"
	"testing"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
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
	start uint64,
	end uint64,
	height uint8,
	bfactor int,
	times [][]int64,
) *MemTree {
	t.Helper()
	version := uint64(1)
	trees := make([]*MemTree, len(times))
	for i, batch := range times {
		root := nodestore.NewInnerNode(height, start, end, bfactor)
		buf := &bytes.Buffer{}
		mcap.WriteFile(t, buf, batch)
		schema := GetSchema(t, bytes.NewReader(buf.Bytes()))
		hash := util.CryptographicHash(schema.Data)
		stats := map[string]*nodestore.Statistics{
			hash: {
				MessageCount:    int64(len(batch)),
				MaxObservedTime: util.Reduce(util.Max, 0, batch),
				MinObservedTime: util.Reduce(util.Min, math.MaxInt64, batch),
			},
		}
		tmp, err := NewInsertBranch(ctx, root, version, uint64(batch[0]*1e9), buf.Bytes(), stats)
		require.NoError(t, err)
		version++
		trees[i] = tmp
	}
	root := nodestore.NewInnerNode(height, start, end, bfactor)
	dest := NewMemTree(nodestore.RandomNodeID(), root)
	if len(times) == 0 {
		return dest
	}
	if len(times) == 1 {
		return trees[0]
	}
	output, err := MergeBranchesInto(ctx, dest, trees...)
	require.NoError(t, err)
	return output
}
