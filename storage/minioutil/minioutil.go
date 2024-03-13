package minioutil

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/minio/madmin-go"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	minio "github.com/minio/minio/cmd"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/testutils"
)

const testBucket = "test"

// NewServer starts a minio server on a random port, and returns a client and
// bucket name to use in tests. The third return value is a function that will
// tear the server down.
func NewServer(t *testing.T) (*mclient.Client, string, func()) {
	t.Helper()
	ctx := context.Background()
	port, err := testutils.GetOpenPort()
	require.NoError(t, err)

	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"

	addr := fmt.Sprintf("localhost:%d", port)

	madm, err := madmin.New(addr, accessKeyID, secretAccessKey, false)
	require.NoError(t, err)

	tmpdir, err := os.MkdirTemp("", "dp3-minio")
	require.NoError(t, err)

	go func() {
		minio.Main([]string{"minio", "server", "--quiet", "--address", addr, tmpdir})
	}()

	// wait for the server to come up
	start := time.Now()
	for {
		_, err := madm.ServerInfo(ctx)
		if err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Error("timeout waiting for minio server to start")
		}
		time.Sleep(100 * time.Millisecond)
	}

	mc, err := mclient.New(addr, &mclient.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})
	require.NoError(t, err)

	require.NoError(t, mc.MakeBucket(ctx, testBucket, mclient.MakeBucketOptions{}))

	return mc, testBucket, func() {
		err1 := madm.ServiceStop(ctx)
		err2 := os.RemoveAll(tmpdir)
		require.NoError(t, err1)
		require.NoError(t, err2)
	}
}
