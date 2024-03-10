package tree_test

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
)

func TestStatRange(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion    string
		messageTimes []uint64
		start        uint64
		end          uint64
		granularity  uint64
		expected     []tree.StatRange
	}{
		{
			"empty tree",
			[]uint64{},
			0,
			100e9,
			1e9,
			[]tree.StatRange{},
		},
		{
			"single insert",
			[]uint64{100},
			0,
			100e9,
			600e9,
			[]tree.StatRange{
				{
					Start:      64e9,
					End:        128e9,
					Statistics: &nodestore.Statistics{MessageCount: 1},
				},
			},
		},
		{
			"inserts spanning buckets",
			[]uint64{100, 4097},
			0,
			5000e9,
			600e9,
			[]tree.StatRange{
				{Start: 4096000000000, End: 4160000000000, Statistics: &nodestore.Statistics{MessageCount: 1}},
				{Start: 64000000000, End: 128000000000, Statistics: &nodestore.Statistics{MessageCount: 1}},
			},
		},
		{
			"inserts spanning disjoint buckets",
			[]uint64{100, 262143},
			0,
			math.MaxUint64,
			600e9,
			[]tree.StatRange{
				{Start: 262080000000000, End: 262144000000000, Statistics: &nodestore.Statistics{MessageCount: 1}},
				{Start: 64000000000, End: 128000000000, Statistics: &nodestore.Statistics{MessageCount: 1}},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ns := nodestore.MockNodestore(ctx, t)
			rootID, err := ns.NewRoot(ctx, 0, util.Pow(uint64(64), 3), 64, 64)
			require.NoError(t, err)
			version := uint64(0)
			roots := make([]nodestore.NodeID, len(c.messageTimes))
			for i, time := range c.messageTimes {
				buf := &bytes.Buffer{}
				mcap.WriteFile(t, buf, []uint64{time})
				version++
				rootID, path, err := tree.Insert(
					ctx, ns, rootID, version, time*1e9, buf.Bytes(),
					&nodestore.Statistics{
						MessageCount: 1,
					},
				)
				require.NoError(t, err)
				require.NoError(t, ns.WALFlush(ctx, "producer", "topic", version, path))
				roots[i] = rootID
			}
			rootID, err = ns.WALMerge(ctx, rootID, version, roots)
			require.NoError(t, err)

			statrange, err := tree.GetStatRange(ctx, ns, rootID, c.start, c.end, c.granularity)
			require.NoError(t, err)
			require.Equal(t, c.expected, statrange)
		})
	}
}
