package util_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util"
)

func TestWithContext(t *testing.T) {
	ctx := context.Background()
	t.Run("inc context value", func(t *testing.T) {
		ctx := util.WithContext(ctx, "test")
		util.IncContextValue(ctx, "test", 1)
		metadata, err := util.MetadataFromContext(ctx)
		require.NoError(t, err)
		require.Equal(t, "test", metadata.Name)
		require.Equal(t,
			`{"name":"test","values":{"test":1},"data":{},"children":null}`,
			metadata.Metadata["context"],
		)
	})

	t.Run("dec value", func(t *testing.T) {
		ctx := util.WithContext(ctx, "test")
		util.DecContextValue(ctx, "test", 1)
		metadata, err := util.MetadataFromContext(ctx)
		require.NoError(t, err)
		require.Equal(t,
			`{"name":"test","values":{"test":-1},"data":{},"children":null}`,
			metadata.Metadata["context"],
		)
	})

	t.Run("set value", func(t *testing.T) {
		ctx := util.WithContext(ctx, "test")
		util.SetContextValue(ctx, "test", 10)
		metadata, err := util.MetadataFromContext(ctx)
		require.NoError(t, err)
		require.Equal(t,
			`{"name":"test","values":{"test":10},"data":{},"children":null}`,
			metadata.Metadata["context"],
		)
	})

	t.Run("set data", func(t *testing.T) {
		ctx := util.WithContext(ctx, "test")
		util.SetContextData(ctx, "test", "data")
		metadata, err := util.MetadataFromContext(ctx)
		require.NoError(t, err)
		require.Equal(t,
			`{"name":"test","values":{},"data":{"test":"data"},"children":null}`,
			metadata.Metadata["context"],
		)
	})

	t.Run("with child context", func(t *testing.T) {
		pctx := util.WithContext(ctx, "test")
		ctx, _ := util.WithChildContext(pctx, "child")
		util.IncContextValue(ctx, "test", 1)
		metadata, err := util.MetadataFromContext(pctx)
		require.NoError(t, err)
		require.Equal(t,
			`{"name":"test","values":{},"data":{},"children":[{"name":"child","values":{"test":1},"data":{},"children":null}]}`,
			metadata.Metadata["context"],
		)
	})
}
