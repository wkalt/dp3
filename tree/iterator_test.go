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

			it, err := tree.NewTreeIterator(ctx, tr, 0, 128*1e9)
			require.NoError(t, err)
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
