package mcap_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/nodestore"
)

func TestMergeFilterIterator(t *testing.T) {
	cases := []struct {
		assertion string
		filter    [][]int
		inputs    [][][]int

		output [][]int
	}{
		{
			"no filter no dupes",
			nil,
			[][][]int{
				{{1, 1}, {2, 2}, {3, 3}},
				{{1, 4}, {2, 5}, {3, 6}},
			},
			[][]int{{1, 1}, {1, 4}, {2, 2}, {2, 5}, {3, 3}, {3, 6}},
		},
		{
			"no filter with dupes is deduplicated",
			nil,
			[][][]int{
				{{1, 1}},
				{{1, 1}},
			},
			[][]int{{1, 1}},
		},
		{
			"filter with no dupes excludes filter",
			[][]int{{1, 1}},
			[][][]int{
				{{1, 1}, {2, 2}},
			},
			[][]int{{2, 2}},
		},
		{
			"filter with dupes excludes all filtered dupes",
			[][]int{{1, 1}},
			[][][]int{
				{{1, 1}, {1, 1}, {2, 2}},
			},
			[][]int{{2, 2}},
		},
		{
			"merge sequences of different lengths",
			nil,
			[][][]int{
				{{1, 1}, {2, 2}, {3, 3}},
				{{1, 4}, {2, 5}},
			},
			[][]int{{1, 1}, {1, 4}, {2, 2}, {2, 5}, {3, 3}},
		},
		{
			"empty input sequence is tolerated",
			nil,
			[][][]int{
				{},
				{{1, 1}, {2, 2}, {3, 3}},
				{{1, 4}, {2, 5}},
			},
			[][]int{{1, 1}, {1, 4}, {2, 2}, {2, 5}, {3, 3}},
		},
		{
			"timestamp collisions ordered on sequence",
			nil,
			[][][]int{
				{{1, 4}},
				{{1, 1}},
			},
			[][]int{{1, 1}, {1, 4}},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			var inputs []mcap.MessageIterator
			for _, input := range c.inputs {
				inputs = append(inputs, mcap.NewMockIterator("topic", input))
			}
			filter := []nodestore.MessageKey{}
			for _, f := range c.filter {
				filter = append(filter, nodestore.MessageKey{Timestamp: uint64(f[0]), Sequence: uint32(f[1])})
			}
			iter, err := mcap.NewFilterMergeIterator(filter, inputs...)
			require.NoError(t, err)

			var actual [][]int
			for {
				_, _, msg, err := iter.Next(nil)
				if err != nil {
					break
				}
				actual = append(actual, []int{int(msg.LogTime), int(msg.Sequence)})
			}
			require.Equal(t, c.output, actual)
		})
	}
}
