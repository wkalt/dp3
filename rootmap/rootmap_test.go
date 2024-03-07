package rootmap_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
)

func TestRootmaps(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	cases := []struct {
		assertion string
		f         func(*testing.T) rootmap.Rootmap
	}{
		{
			"mem",
			func(t *testing.T) rootmap.Rootmap {
				t.Helper()
				return rootmap.NewMemRootmap()
			},
		},
		{
			"sql",
			func(t *testing.T) rootmap.Rootmap {
				t.Helper()
				rm, err := rootmap.NewSQLRootmap(db)
				require.NoError(t, err)
				return rm
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			rm := c.f(t)
			t.Run("put", func(t *testing.T) {
				streamID := uuid.New().String()
				expected := randNodeID()
				err := rm.Put(ctx, streamID, 10, expected)
				require.NoError(t, err)

				nodeID, err := rm.Get(ctx, streamID, 10)
				require.NoError(t, err)
				require.Equal(t, expected, nodeID)
			})

			t.Run("get latest", func(t *testing.T) {
				streamID := uuid.New().String()
				node1 := randNodeID()
				err := rm.Put(ctx, streamID, 10, node1)
				require.NoError(t, err)

				node2 := randNodeID()
				err = rm.Put(ctx, streamID, 20, node2)
				require.NoError(t, err)

				nodeID, err := rm.GetLatest(ctx, streamID)
				require.NoError(t, err)
				require.Equal(t, node2, nodeID)
			})
		})
	}
}

func randNodeID() nodestore.NodeID {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	var id nodestore.NodeID
	copy(id[:], buf)
	return id
}
