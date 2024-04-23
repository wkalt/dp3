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

	var testPrefix = "test"

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
				err := rm.Put(ctx, "db", "my-device", "my-topic", 10, testPrefix, expected)
				require.NoError(t, err)

				prefix, nodeID, err := rm.Get(ctx, "db", "my-device", "my-topic", 10)
				require.NoError(t, err)
				require.Equal(t, expected, nodeID)
				require.Equal(t, testPrefix, prefix)
			})

			t.Run("get latest", func(t *testing.T) {
				node1 := randNodeID()
				err := rm.Put(ctx, "db", "my-device", "my-topic", 20, testPrefix, node1)
				require.NoError(t, err)

				node2 := randNodeID()
				err = rm.Put(ctx, "db", "my-device", "my-topic", 30, testPrefix, node2)
				require.NoError(t, err)

				prefix, nodeID, _, err := rm.GetLatest(ctx, "db", "my-device", "my-topic")
				require.NoError(t, err)
				require.Equal(t, node2, nodeID)
				require.Equal(t, testPrefix, prefix)
			})
			t.Run("get latest by topic", func(t *testing.T) {
				node1 := randNodeID()
				err := rm.Put(ctx, "db", "my-device", "topic1", 40, testPrefix, node1)
				require.NoError(t, err)

				node2 := randNodeID()
				err = rm.Put(ctx, "db", "my-device", "topic2", 50, testPrefix, node2)
				require.NoError(t, err)

				listings, err := rm.GetLatestByTopic(ctx, "db", "my-device", map[string]uint64{"topic1": 0, "topic2": 0})
				require.NoError(t, err)

				converted := make([]rootmap.RootListing, 0, len(listings))
				for _, listing := range listings {
					listing.Timestamp = ""
					converted = append(converted, listing)
				}
				require.ElementsMatch(t, []rootmap.RootListing{
					{testPrefix, "my-device", "topic1", node1, 40, "", 0},
					{testPrefix, "my-device", "topic2", node2, 50, "", 0},
				}, converted)
			})
			t.Run("get version that does not exist", func(t *testing.T) {
				_, _, err := rm.Get(ctx, "db", "fake-device", "my-topic", 1e9)
				require.ErrorIs(t, err, rootmap.TableNotFoundError{})
			})
			t.Run("get latest version that does not exist", func(t *testing.T) {
				_, _, _, err := rm.GetLatest(ctx, "db", "fake-device", "my-topic")
				require.ErrorIs(t, err, rootmap.TableNotFoundError{})
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
