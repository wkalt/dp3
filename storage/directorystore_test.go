package storage_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/storage"
)

func TestDirectoryStore(t *testing.T) {
	dir, err := os.MkdirTemp("", "dp3")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	store := storage.NewDirectoryStore(dir)
	t.Run("put and get", func(t *testing.T) {
		require.NoError(t, store.Put("a", []byte("a")))
		value, err := store.GetRange("a", 0, 1)
		require.NoError(t, err)
		require.Equal(t, []byte("a"), value)
	})
	t.Run("delete", func(t *testing.T) {
		require.NoError(t, store.Delete("a"))
		_, err := store.GetRange("a", 0, 1)
		require.ErrorIs(t, err, storage.ErrObjectNotFound)
	})
}
