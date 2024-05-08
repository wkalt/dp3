package rootmap_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
)

func TestRootmapPutGet(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		assertion string
		f         func(*testing.T, rootmap.Rootmap)
	}{
		{
			"get valid put",
			func(t *testing.T, rm rootmap.Rootmap) {
				t.Helper()
				expected := randNodeID()
				err := rm.Put(ctx, "db", "my-device", "my-topic", 10, "test", expected)
				require.NoError(t, err)

				prefix, nodeID, err := rm.Get(ctx, "db", "my-device", "my-topic", 10)
				require.NoError(t, err)
				require.Equal(t, expected, nodeID)
				require.Equal(t, "test", prefix)
			},
		},
		{
			"get version that does not exist",
			func(t *testing.T, rm rootmap.Rootmap) {
				t.Helper()
				_, _, err := rm.Get(ctx, "db", "fake-device", "my-topic", 1e9)
				require.ErrorIs(t, err, rootmap.TableNotFoundError{})
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
			require.NoError(t, err)
			defer db.Close()
			rm, err := rootmap.NewSQLRootmap(ctx, db, rootmap.WithReservationSize(1000))
			require.NoError(t, err)
			c.f(t, rm)
		})
	}
}

func TestRootmapGetLatest(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		assertion string
		f         func(*testing.T, rootmap.Rootmap)
	}{
		{
			"get latest",
			func(t *testing.T, rm rootmap.Rootmap) {
				t.Helper()
				node1 := randNodeID()
				err := rm.Put(ctx, "db", "my-device", "my-topic", 20, "test", node1)
				require.NoError(t, err)

				node2 := randNodeID()
				err = rm.Put(ctx, "db", "my-device", "my-topic", 30, "test", node2)
				require.NoError(t, err)

				prefix, nodeID, _, _, err := rm.GetLatest(ctx, "db", "my-device", "my-topic")
				require.NoError(t, err)
				require.Equal(t, node2, nodeID)
				require.Equal(t, "test", prefix)
			},
		},
		{
			"get latest version that does not exist",
			func(t *testing.T, rm rootmap.Rootmap) {
				t.Helper()
				_, _, _, _, err := rm.GetLatest(ctx, "db", "fake-device", "my-topic")
				require.ErrorIs(t, err, rootmap.TableNotFoundError{})
			},
		},
		{
			"latest version is not truncated (exceeds truncation version)",
			func(t *testing.T, rm rootmap.Rootmap) {
				t.Helper()
				node1 := randNodeID()
				err := rm.Put(ctx, "db", "my-device", "my-topic", 20, "test", node1)
				require.NoError(t, err)

				t1 := time.Now()

				node2 := randNodeID()
				err = rm.Put(ctx, "db", "my-device", "my-topic", 30, "test", node2)
				require.NoError(t, err)

				err = rm.Truncate(ctx, "db", "my-device", "my-topic", t1.UnixNano())
				require.NoError(t, err)

				var version, truncationVerison uint64
				_, _, version, truncationVerison, err = rm.GetLatest(ctx, "db", "my-device", "my-topic")
				require.NoError(t, err)
				require.Equal(t, uint64(30), version)
				require.Equal(t, uint64(20), truncationVerison)
			},
		},
		{
			"latest version is truncated (does not exceed truncation version)",
			func(t *testing.T, rm rootmap.Rootmap) {
				t.Helper()
				node1 := randNodeID()
				err := rm.Put(ctx, "db", "my-device", "my-topic", 20, "test", node1)
				require.NoError(t, err)

				node2 := randNodeID()
				err = rm.Put(ctx, "db", "my-device", "my-topic", 30, "test", node2)
				require.NoError(t, err)

				t1 := time.Now()
				err = rm.Truncate(ctx, "db", "my-device", "my-topic", t1.UnixNano())
				require.NoError(t, err)

				var version, truncationVerison uint64
				_, _, version, truncationVerison, err = rm.GetLatest(ctx, "db", "my-device", "my-topic")
				require.NoError(t, err)
				require.Equal(t, uint64(30), version)
				require.Equal(t, uint64(30), truncationVerison)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
			require.NoError(t, err)
			defer db.Close()
			rm, err := rootmap.NewSQLRootmap(ctx, db, rootmap.WithReservationSize(1000))
			require.NoError(t, err)
			c.f(t, rm)
		})
	}
}

func TestRootmapGetHistorical(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		assertion string
		f         func(*testing.T, rootmap.Rootmap)
	}{
		{
			"get historical",
			func(t *testing.T, rm rootmap.Rootmap) {
				t.Helper()
				node1 := randNodeID()
				err := rm.Put(ctx, "db", "my-device", "my-topic", 20, "test", node1)
				require.NoError(t, err)

				node2 := randNodeID()
				err = rm.Put(ctx, "db", "my-device", "my-topic", 30, "test", node2)
				require.NoError(t, err)

				listings, err := rm.GetHistorical(ctx, "db", "my-device", "my-topic")
				require.NoError(t, err)

				require.ElementsMatch(t, []rootmap.RootListing{
					{"test", "my-device", "my-topic", node1, 20, "", 0},
					{"test", "my-device", "my-topic", node2, 30, "", 0},
				}, convertListings(listings))
			},
		},
		{
			"get historical with truncation",
			func(t *testing.T, rm rootmap.Rootmap) {
				t.Helper()
				node1 := randNodeID()
				err := rm.Put(ctx, "db", "my-device", "my-topic", 20, "test", node1)
				require.NoError(t, err)

				t1 := time.Now()

				node2 := randNodeID()
				err = rm.Put(ctx, "db", "my-device", "my-topic", 30, "test", node2)
				require.NoError(t, err)

				err = rm.Truncate(ctx, "db", "my-device", "my-topic", t1.UnixNano())
				require.NoError(t, err)

				listings, err := rm.GetHistorical(ctx, "db", "my-device", "my-topic")
				require.NoError(t, err)

				require.ElementsMatch(t, []rootmap.RootListing{
					{"test", "my-device", "my-topic", node2, 30, "", 21},
				}, convertListings(listings))
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
			require.NoError(t, err)
			defer db.Close()
			rm, err := rootmap.NewSQLRootmap(ctx, db, rootmap.WithReservationSize(1000))
			require.NoError(t, err)
			c.f(t, rm)
		})
	}
}

func TestRootmapGetLatestByTopic(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		assertion string
		f         func(*testing.T, rootmap.Rootmap)
	}{
		{
			"get latest by topic",
			func(t *testing.T, rm rootmap.Rootmap) {
				t.Helper()
				node1 := randNodeID()
				err := rm.Put(ctx, "db", "my-device", "topic1", 40, "test", node1)
				require.NoError(t, err)

				node2 := randNodeID()
				err = rm.Put(ctx, "db", "my-device", "topic2", 50, "test", node2)
				require.NoError(t, err)

				listings, err := rm.GetLatestByTopic(ctx, "db", "my-device", map[string]uint64{"topic1": 0, "topic2": 0})
				require.NoError(t, err)

				require.ElementsMatch(t, []rootmap.RootListing{
					{"test", "my-device", "topic1", node1, 40, "", 0},
					{"test", "my-device", "topic2", node2, 50, "", 0},
				}, convertListings(listings))
			},
		},
		{
			"get latest by topic with truncation",
			func(t *testing.T, rm rootmap.Rootmap) {
				t.Helper()
				node1 := randNodeID()
				err := rm.Put(ctx, "db", "my-device", "topic1", 40, "test", node1)
				require.NoError(t, err)

				t1 := time.Now()

				node2 := randNodeID()
				err = rm.Put(ctx, "db", "my-device", "topic1", 50, "test", node2)
				require.NoError(t, err)

				node3 := randNodeID()
				err = rm.Put(ctx, "db", "my-device", "topic2", 60, "test", node3)
				require.NoError(t, err)

				err = rm.Truncate(ctx, "db", "my-device", "topic1", t1.UnixNano())
				require.NoError(t, err)

				listings, err := rm.GetLatestByTopic(ctx, "db", "my-device", map[string]uint64{"topic1": 0, "topic2": 0})
				require.NoError(t, err)

				require.ElementsMatch(t, []rootmap.RootListing{
					{"test", "my-device", "topic1", node2, 50, "", 41},
					{"test", "my-device", "topic2", node3, 60, "", 0},
				}, convertListings(listings))
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
			require.NoError(t, err)
			defer db.Close()
			rm, err := rootmap.NewSQLRootmap(ctx, db, rootmap.WithReservationSize(1000))
			require.NoError(t, err)
			c.f(t, rm)
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

func convertListings(listings []rootmap.RootListing) []rootmap.RootListing {
	converted := make([]rootmap.RootListing, 0, len(listings))
	for _, listing := range listings {
		listing.Timestamp = ""
		converted = append(converted, listing)
	}
	return converted
}
