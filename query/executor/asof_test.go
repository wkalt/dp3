package executor_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/query/executor"
)

func TestAsofJoinNode(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		left      executor.Node
		right     executor.Node
		immediate bool
		threshold uint64
		expected  []uint64
	}{
		{
			"simple asof join",
			executor.NewMockNode(1, 5, 10),
			executor.NewMockNode(4, 7, 15),
			true,
			0,
			[]uint64{1, 4, 5, 7, 10, 15},
		},
		{
			"only returns the latest prior element",
			executor.NewMockNode(1, 3, 5, 10),
			executor.NewMockNode(4, 7, 15),
			true,
			0,
			[]uint64{3, 4, 5, 7, 10, 15},
		},
		{
			"only returns the latest prior element with threshold",
			executor.NewMockNode(1, 3, 4, 10),
			executor.NewMockNode(4, 7, 15),
			true,
			2,
			[]uint64{3, 4},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			node := executor.NewAsofJoinNode(c.left, c.right, c.immediate, c.threshold)
			actual := []uint64{}
			for {
				tuple, err := node.Next(ctx)
				if err != nil {
					break
				}
				actual = append(actual, tuple.Message.LogTime)
			}
			require.Equal(t, c.expected, actual)
			require.NoError(t, node.Close())
		})
	}
}
