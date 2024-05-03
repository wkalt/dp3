package rootmap_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/rootmap"
)

func TestVersionStore(t *testing.T) {
	ctx := context.Background()
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	vs := rootmap.NewSQLVersionStore(ctx, 1000)
	t.Run("next version", func(t *testing.T) {
		last := uint64(0)
		for i := 0; i < 1e6; i++ {
			version, err := vs.NextVersion(ctx, db)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			require.Greater(t, version, last)
			last = version
		}
	})
}
