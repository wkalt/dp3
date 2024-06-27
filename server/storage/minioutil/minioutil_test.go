package minioutil_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/storage/minioutil"
)

func TestWithMinio(t *testing.T) {
	ctx := context.Background()
	mc, bucket, clear := minioutil.NewServer(t)
	defer clear()

	data := []byte("hello")
	n := int64(len(data))

	t.Run("writing", func(t *testing.T) {
		_, err := mc.PutObject(ctx, bucket, "test", bytes.NewReader(data), n, minio.PutObjectOptions{})
		require.NoError(t, err)
	})

	t.Run("reading", func(t *testing.T) {
		_, err := mc.PutObject(ctx, bucket, "test2", bytes.NewReader(data), n, minio.PutObjectOptions{})
		require.NoError(t, err)

		obj, err := mc.GetObject(ctx, bucket, "test2", minio.GetObjectOptions{})
		require.NoError(t, err)

		buf := &bytes.Buffer{}
		_, err = buf.ReadFrom(obj)
		require.NoError(t, err)
	})
}
