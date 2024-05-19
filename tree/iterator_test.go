package tree_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
)

func TestTreeIterator(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion     string
		times         [][]int64
		expectedTimes []uint64
	}{
		{
			"empty tree",
			[][]int64{},
			[]uint64{},
		},
		{
			"one message",
			[][]int64{{100}},
			[]uint64{100},
		},
		{
			"multiple messages on the same leaf",
			[][]int64{{16, 32, 48}},
			[]uint64{16, 32, 48},
		},
		{
			"multiple messages on the same leaf (separate writes)",
			[][]int64{{0, 16}, {32, 48}, {20, 40}},
			[]uint64{0, 16, 20, 32, 40, 48},
		},
		{
			"multiple messages on different leaves",
			[][]int64{{100}, {1000}, {2000}},
			[]uint64{100, 1000, 2000},
		},
	}
	for _, c := range cases {
		for _, descending := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s%v", c.assertion, util.When(descending, " descending", "")), func(t *testing.T) {
				if descending {
					slices.Reverse(c.expectedTimes)
				}
				actualTimes := []uint64{}
				tr := tree.MergeInserts(ctx, t, 0, util.Pow(uint64(64), 3), 2, 64, c.times)
				it := tree.NewTreeIterator(ctx, tr, descending, 0, math.MaxUint64, 0, nil)
				for {
					_, _, message, err := it.Next(ctx)
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
					actualTimes = append(actualTimes, message.LogTime)
				}
				require.NoError(t, it.Close())
				require.Equal(t, c.expectedTimes, actualTimes)
			})
		}
	}
}

func TestSplitRangeSet(t *testing.T) {
	cases := []struct {
		assertion string
		input     [][]uint64
		split     []uint64
		expected  [][]uint64
	}{
		{
			"split on left side",
			[][]uint64{{10, 20}},
			[]uint64{0, 15},
			[][]uint64{{15, 20}},
		},
		{
			"split on right side",
			[][]uint64{{10, 20}},
			[]uint64{15, 25},
			[][]uint64{{10, 15}},
		},
		{
			"split in the middle",
			[][]uint64{{10, 20}},
			[]uint64{15, 17},
			[][]uint64{{10, 15}, {17, 20}},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			require.Equal(t, c.expected, tree.SplitRangeSet(c.input, c.split[0], c.split[1]))
		})
	}
}
