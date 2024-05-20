package executor_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/executor"
)

func TestOffsetNode(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		child     executor.Node
		offset    int
		expected  []uint64
	}{
		{
			"offset 0",
			executor.NewMockNode(1, 2, 3),
			0,
			[]uint64{1, 2, 3},
		},
		{
			"offset 1",
			executor.NewMockNode(1, 2, 3),
			1,
			[]uint64{2, 3},
		},
		{
			"offset 2",
			executor.NewMockNode(1, 2, 3),
			2,
			[]uint64{3},
		},
		{
			"offset 3",
			executor.NewMockNode(1, 2, 3),
			3,
			[]uint64{},
		},
		{
			"offset 0 with empty node",
			executor.NewMockNode(),
			0,
			[]uint64{},
		},
		{
			"offset 1 with empty node",
			executor.NewMockNode(),
			1,
			[]uint64{},
		},
		{
			"offset 2 with empty node",
			executor.NewMockNode(),
			2,
			[]uint64{},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			n := executor.NewOffsetNode(c.offset, c.child)
			actual := []uint64{}
			for {
				tuple, err := n.Next(ctx)
				if err != nil {
					break
				}
				actual = append(actual, tuple.LogTime())
			}
			require.Equal(t, c.expected, actual)
			require.NoError(t, n.Close(ctx))
		})
	}
}
