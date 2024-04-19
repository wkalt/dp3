package executor_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/executor"
)

func TestMergeNode(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion  string
		descending bool
		children   []executor.Node
		expected   []uint64
	}{
		{
			"single node",
			false,
			[]executor.Node{
				executor.NewMockNode(1, 2, 3),
			},
			[]uint64{1, 2, 3},
		},
		{
			"single node descending",
			true,
			[]executor.Node{
				executor.NewMockNode(3, 2, 1),
			},
			[]uint64{3, 2, 1},
		},
		{
			"two nodes",
			false,
			[]executor.Node{
				executor.NewMockNode(1, 3, 5),
				executor.NewMockNode(2, 4, 6),
			},
			[]uint64{1, 2, 3, 4, 5, 6},
		},
		{
			"two nodes descending",
			true,
			[]executor.Node{
				executor.NewMockNode(5, 3, 1),
				executor.NewMockNode(6, 4, 2),
			},
			[]uint64{6, 5, 4, 3, 2, 1},
		},
		{
			"three nodes",
			false,
			[]executor.Node{
				executor.NewMockNode(1, 4, 7),
				executor.NewMockNode(2, 5, 8),
				executor.NewMockNode(3, 6, 9),
			},
			[]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"empty nodes",
			false,
			[]executor.Node{
				executor.NewMockNode(),
				executor.NewMockNode(),
				executor.NewMockNode(),
			},
			[]uint64{},
		},
		{
			"one nonempty node",
			false,
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
			node := executor.NewMergeNode(c.descending, c.children...)
			actual := []uint64{}
			for {
				tuple, err := node.Next(ctx)
				if err != nil {
					require.ErrorIs(t, err, io.EOF)
					break
				}
				actual = append(actual, tuple.LogTime())
			}
			require.Equal(t, c.expected, actual)
			require.NoError(t, node.Close())
		})
	}
}
