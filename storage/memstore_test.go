package storage_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/storage"
)

func TestMemstore(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStore()
	t.Run("put and get", func(t *testing.T) {
		require.NoError(t, store.Put(ctx, "a", []byte("a")))
		value, err := store.GetRange(ctx, "a", 0, 1)
		require.NoError(t, err)
		assert.Equal(t, value, []byte("a"))
	})
	t.Run("delete", func(t *testing.T) {
		require.NoError(t, store.Delete(ctx, "a"))
		_, err := store.GetRange(ctx, "a", 0, 1)
		assert.ErrorIs(t, err, storage.ErrObjectNotFound)
	})
}
