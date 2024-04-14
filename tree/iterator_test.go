package tree_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/tree"
)

func TestTreeIterator(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		assertion            string
		times                [][]int64
		expectedMessageCount int
	}{
		{
			"empty tree",
			[][]int64{},
			0,
		},
		{
			"one message",
			[][]int64{{100}},
			1,
		},
		{
			"three messages",
			[][]int64{{100, 1000, 10000}},
			3,
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			tr := tree.MergeInserts(ctx, t, 0, 128*1e9, 2, 64, c.times)

			it := tree.NewTreeIterator(ctx, tr, 0, 128*1e9, 0)
			var count int
			for {
				_, _, _, err := it.Next(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				count += 1
			}
			require.Equal(t, c.expectedMessageCount, count)
			require.NoError(t, it.Close())
		})
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
