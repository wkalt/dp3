package executor_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/query/executor"
)

func TestMergeNode(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		children  []executor.Node
		expected  []uint64
	}{
		{
			"single node",
			[]executor.Node{
				executor.NewMockNode(1, 2, 3),
			},
			[]uint64{1, 2, 3},
		},
		{
			"two nodes",
			[]executor.Node{
				executor.NewMockNode(1, 3, 5),
				executor.NewMockNode(2, 4, 6),
			},
			[]uint64{1, 2, 3, 4, 5, 6},
		},
		{
			"three nodes",
			[]executor.Node{
				executor.NewMockNode(1, 4, 7),
				executor.NewMockNode(2, 5, 8),
				executor.NewMockNode(3, 6, 9),
			},
			[]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"empty nodes",
			[]executor.Node{
				executor.NewMockNode(),
				executor.NewMockNode(),
				executor.NewMockNode(),
			},
			[]uint64{},
		},
		{
			"one nonempty node",
			[]executor.Node{
				executor.NewMockNode(1, 2, 3),
				executor.NewMockNode(),
				executor.NewMockNode(),
			},
			[]uint64{1, 2, 3},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			node := executor.NewMergeNode(c.children...)
			actual := []uint64{}
			for {
				tuple, err := node.Next(ctx)
				if err != nil {
					require.ErrorIs(t, err, io.EOF)
					break
				}
				actual = append(actual, tuple.Message.LogTime)
			}
			require.Equal(t, c.expected, actual)
			require.NoError(t, node.Close())
		})
	}
}
