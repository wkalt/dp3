package storage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/storage"
)

func TestMemstore(t *testing.T) {
	store := storage.NewMemStore()
	t.Run("put and get", func(t *testing.T) {
		store.Put(1, []byte("a"))
		value, err := store.Get(1)
		require.NoError(t, err)
		assert.Equal(t, value, []byte("a"))
	})
	t.Run("delete", func(t *testing.T) {
		store.Delete(1)
		_, err := store.Get(1)
		assert.ErrorIs(t, err, storage.ErrObjectNotFound)
	})
}
