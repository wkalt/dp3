package versionstore_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/versionstore"
)

func TestVersionStore(t *testing.T) {
	ctx := context.Background()
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	vs := versionstore.NewVersionStore(ctx, db, 1000)
	t.Run("next version", func(t *testing.T) {
		last := uint64(0)
		for i := 0; i < 1e6; i++ {
			version, err := vs.NextVersion(ctx)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			require.Greater(t, version, last)
			last = version
		}
	})
}
