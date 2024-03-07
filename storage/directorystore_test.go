package storage_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/storage"
)

func TestDirectoryStore(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "dp3")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	store := storage.NewDirectoryStore(dir)
	t.Run("put and get", func(t *testing.T) {
		require.NoError(t, store.Put(ctx, "a", []byte("a")))
		value, err := store.GetRange(ctx, "a", 0, 1)
		require.NoError(t, err)
		require.Equal(t, []byte("a"), value)
	})
	t.Run("delete", func(t *testing.T) {
		require.NoError(t, store.Delete(ctx, "a"))
		_, err := store.GetRange(ctx, "a", 0, 1)
		require.ErrorIs(t, err, storage.ErrObjectNotFound)
	})
}
