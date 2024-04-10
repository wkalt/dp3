package executor_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/query/executor"
)

func TestLimitNode(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		limit     int
		child     executor.Node
		expected  []uint64
	}{
		{
			"limit 0",
			0,
			executor.NewMockNode(1, 2, 3),
			[]uint64{},
		},
		{
			"limit 1",
			1,
			executor.NewMockNode(1, 2, 3),
			[]uint64{1},
		},
		{
			"limit 2",
			2,
			executor.NewMockNode(1, 2, 3),
			[]uint64{1, 2},
		},
		{
			"limit exceeds size of resultset",
			4,
			executor.NewMockNode(1, 2, 3),
			[]uint64{1, 2, 3},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			node := executor.NewLimitNode(c.limit, c.child)
			actual := []uint64{}
			for {
				tuple, err := node.Next(ctx)
				if err != nil {
					break
				}
				actual = append(actual, tuple.Message.LogTime)
			}
			require.Equal(t, c.expected, actual)
		})
	}
}
