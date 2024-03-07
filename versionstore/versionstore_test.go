package versionstore_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/versionstore"
)

func TestVersionStore(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		f         func(*testing.T) versionstore.Versionstore
	}{
		{
			"mem",
			func(_ *testing.T) versionstore.Versionstore {
				return versionstore.NewMemVersionStore()
			},
		},
		{
			"sql",
			func(t *testing.T) versionstore.Versionstore {
				t.Helper()
				db, err := sql.Open("sqlite3", ":memory:")
				require.NoError(t, err)
				return versionstore.NewSQLVersionstore(db, 1000)
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			vs := c.f(t)
			t.Run("next", func(t *testing.T) {
				last := uint64(0)
				for i := 0; i < 1e6; i++ {
					version, err := vs.Next(ctx)
					if err != nil {
						t.Errorf("unexpected error: %v", err)
					}
					require.Greater(t, version, last)
					last = version
				}
			})
		})
	}
}
