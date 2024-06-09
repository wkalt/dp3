package storage_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/storage/minioutil"
)

func TestStorageProviders(t *testing.T) {
	ctx := context.Background()

	mc, bucket, clear := minioutil.NewServer(t)
	defer clear()

	tmpdir, err := os.MkdirTemp("", "dp3-dirstore")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	dstore, err := storage.NewDirectoryStore(tmpdir)
	require.NoError(t, err)

	cases := []struct {
		assertion string
		store     storage.Provider
	}{
		{
			"s3 store",
			storage.NewS3Store(mc, bucket),
		},
		{
			"memory store",
			storage.NewMemStore(),
		},
		{
			"directory store",
			dstore,
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			t.Run("put", func(t *testing.T) {
				require.NoError(t, c.store.Put(ctx, "test", bytes.NewReader([]byte("hello"))))
			})

			t.Run("get", func(t *testing.T) {
				require.NoError(t, c.store.Put(ctx, "item", bytes.NewReader([]byte("hello"))))
				r, err := c.store.Get(ctx, "item")
				require.NoError(t, err)
				defer r.Close()
				data, err := io.ReadAll(r)
				require.NoError(t, err)
				require.Equal(t, []byte("hello"), data)
			})
			t.Run("get range", func(t *testing.T) {
				require.NoError(t, c.store.Put(ctx, "test2", bytes.NewReader([]byte("hello"))))
				r, err := c.store.GetRange(ctx, "test2", 1, 4)
				require.NoError(t, err)
				defer r.Close()
				data, err := io.ReadAll(r)
				require.NoError(t, err)
				require.Equal(t, []byte("ello"), data)
			})
			t.Run("delete", func(t *testing.T) {
				require.NoError(t, c.store.Put(ctx, "test3", bytes.NewReader([]byte("hello"))))
				require.NoError(t, c.store.Delete(ctx, "test3"))
				_, err := c.store.GetRange(ctx, "test3", 0, 5)
				require.ErrorIs(t, err, storage.ErrObjectNotFound)
			})

			t.Run("get object that does not exist returns error", func(t *testing.T) {
				_, err := c.store.GetRange(ctx, "test4", 0, 4)
				require.ErrorIs(t, err, storage.ErrObjectNotFound)
			})

			t.Run("deleting object that does not exist returns no error", func(t *testing.T) {
				err := c.store.Delete(ctx, "test100")
				require.NoError(t, err)
			})
		})
	}
}
