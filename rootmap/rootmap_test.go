package rootmap_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"testing"

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
				expected := randNodeID()
				err := rm.Put(ctx, "my-device", "my-topic", 10, expected)
				require.NoError(t, err)

				nodeID, err := rm.Get(ctx, "my-device", "my-topic", 10)
				require.NoError(t, err)
				require.Equal(t, expected, nodeID)
			})

			t.Run("get latest", func(t *testing.T) {
				node1 := randNodeID()
				err := rm.Put(ctx, "my-device", "my-topic", 20, node1)
				require.NoError(t, err)

				node2 := randNodeID()
				err = rm.Put(ctx, "my-device", "my-topic", 30, node2)
				require.NoError(t, err)

				nodeID, _, err := rm.GetLatest(ctx, "my-device", "my-topic")
				require.NoError(t, err)
				require.Equal(t, node2, nodeID)
			})
			t.Run("get latest by topic", func(t *testing.T) {
				node1 := randNodeID()
				err := rm.Put(ctx, "my-device", "topic1", 40, node1)
				require.NoError(t, err)

				node2 := randNodeID()
				err = rm.Put(ctx, "my-device", "topic2", 50, node2)
				require.NoError(t, err)

				nodeIDs, _, err := rm.GetLatestByTopic(ctx, "my-device", []string{"topic1", "topic2"})
				require.NoError(t, err)
				require.ElementsMatch(t, []nodestore.NodeID{node1, node2}, nodeIDs)
			})
			t.Run("get version that does not exist", func(t *testing.T) {
				_, err := rm.Get(ctx, "fake-device", "my-topic", 1e9)
				require.ErrorIs(t, err, rootmap.StreamNotFoundError{})
			})
			t.Run("get latest version that does not exist", func(t *testing.T) {
				_, _, err := rm.GetLatest(ctx, "fake-device", "my-topic")
				require.ErrorIs(t, err, rootmap.StreamNotFoundError{})
			})
		})
	}
}

func randNodeID() nodestore.NodeID {
	buf := make([]byte, 24)
	_, _ = rand.Read(buf)
	var id nodestore.NodeID
	copy(id[:], buf)
	return id
}
