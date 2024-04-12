package executor_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/executor"
)

func TestMockNode(t *testing.T) {
	ctx := context.Background()
	for range 1000 {
		node := executor.NewMockNode(1, 2, 3)
		for j := range 3 {
			tuple, err := node.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(j+1), tuple.LogTime())
		}
		_, err := node.Next(ctx)
		require.ErrorIs(t, err, io.EOF)
	}
}
