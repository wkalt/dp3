package treemgr_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/versionstore"
)

func TestGetMessages(t *testing.T) {
}

func TestGetStatistics(t *testing.T) {
}

func TestInsert(t *testing.T) {
}

func TestGetMessagesLatest(t *testing.T) {
	ctx := context.Background()
	f, err := os.Open("/home/wyatt/data/bags/demo.mcap")
	require.NoError(t, err)
	tmgr := testTreeManager(ctx, t)

	require.NoError(t, tmgr.Receive(ctx, "my-device", f))
	require.NoError(t, tmgr.SyncWAL(ctx))

	buf := &bytes.Buffer{}
	require.NoError(t, tmgr.GetMessagesLatest(
		ctx,
		buf,
		util.DateSeconds("2015-01-01")*1e9,
		util.DateSeconds("2020-01-01")*1e9,
		"my-device",
		[]string{"/diagnostics"},
	))

	reader, err := mcap.NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	info, err := reader.Info()
	require.NoError(t, err)
	require.Equal(t, 52, int(info.Statistics.MessageCount))
}

func TestGetStatisticsLatest(t *testing.T) {
}

func TestSyncWAL(t *testing.T) {
}

func removeSpace(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "  ", "")
	s = strings.ReplaceAll(s, "\t", "")
	return s
}

func assertEqualTrees(t *testing.T, a, b string) {
	t.Helper()
	require.Equal(t, removeSpace(a), removeSpace(b), "%s != %s", a, b)
}

func TestReceive(t *testing.T) {
	ctx := context.Background()
	buf := &bytes.Buffer{}

	cases := []struct {
		assertion string
		input     [][]uint64
		output    []string
	}{
		{
			"single-topic file, single message",
			[][]uint64{{10}},
			[]string{
				`[0-64424509440:4 [0-1006632960:4 (count=1) [0-15728640:4 (count=1)
				[0-245760:4 (count=1) [0-3840:4 (count=1) [0-60:4 (count=1) [leaf 1 msg]]]]]]]`,
			},
		},
		{
			"two topics, single messages, nonoverlapping",
			[][]uint64{{10e9}, {100e9}},
			[]string{
				`[0-64424509440:6 [0-1006632960:6 (count=1) [0-15728640:6 (count=1) [0-245760:6 (count=1)
				[0-3840:6 (count=1) [0-60:6 (count=1) [leaf 1 msg]]]]]]]`,
				`[0-64424509440:7 [0-1006632960:7 (count=1) [0-15728640:7 (count=1)
				[0-245760:7 (count=1) [0-3840:7 (count=1) [60-120:7 (count=1) [leaf 1 msg]]]]]]]`,
			},
		},
		{
			"single-topic file, spanning leaf boundaries",
			[][]uint64{{10e9, 100e9}},
			[]string{
				`[0-64424509440:5 [0-1006632960:5 (count=2) [0-15728640:5 (count=2)
				[0-245760:5 (count=2) [0-3840:5 (count=2) [0-60:5 (count=1) [leaf 1 msg]]
				[60-120:5 (count=1) [leaf 1 msg]]]]]]]`,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf.Reset()
			mcap.WriteFile(t, buf, c.input...)
			tmgr := testTreeManager(ctx, t)
			require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
			require.NoError(t, tmgr.SyncWAL(ctx))

			for i := range c.output {
				topic := fmt.Sprintf("topic-%d", i)
				t.Run("comparing"+topic, func(t *testing.T) {
					str := tmgr.PrintStream(ctx, "my-device", topic)
					assertEqualTrees(t, c.output[i], str)
				})
			}
		})
	}
}

func testTreeManager(ctx context.Context, t *testing.T) *treemgr.TreeManager {
	t.Helper()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	wal, err := nodestore.NewSQLWAL(ctx, db)
	require.NoError(t, err)
	ns := nodestore.NewNodestore(store, cache, wal)
	vs := versionstore.NewMemVersionStore()
	rm := rootmap.NewMemRootmap()
	tmgr := treemgr.NewTreeManager(ns, vs, rm, 2)
	return tmgr
}

// func TestStreamingAndIngestion(t *testing.T) {
// 	ctx := context.Background()
// 	store := storage.NewMemStore()
// 	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
// 	db, err := sql.Open("sqlite3", ":memory:")
// 	require.NoError(t, err)
// 	wal, err := nodestore.NewSQLWAL(ctx, db)
// 	require.NoError(t, err)
// 	ns := nodestore.NewNodestore(store, cache, wal)
// 	vs := versionstore.NewMemVersionStore()
// 	rm := rootmap.NewMemRootmap()
// 	tmgr := treemgr.NewTreeManager(ns, vs, rm, 2)
//
// 	f, err := os.Open("/home/wyatt/data/bags/demo.mcap")
// 	require.NoError(t, err)
// 	defer f.Close()
//
// 	require.NoError(t, tmgr.Receive(ctx, "my-device", f))
// 	require.NoError(t, tmgr.SyncWAL(ctx))
//
// 	// can I read
// 	buf := &bytes.Buffer{}
// 	streamID := util.ComputeStreamID("my-device", "/diagnostics")
// 	require.NoError(t, tmgr.GetMessagesLatest(ctx, buf, 0, math.MaxUint64, []string{streamID}))
// }
//
// func TestIngestion(t *testing.T) {
// 	ctx := context.Background()
// 	store := storage.NewMemStore()
// 	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
// 	db, err := sql.Open("sqlite3", ":memory:")
// 	require.NoError(t, err)
// 	wal, err := nodestore.NewSQLWAL(ctx, db)
// 	require.NoError(t, err)
// 	ns := nodestore.NewNodestore(store, cache, wal)
// 	vs := versionstore.NewMemVersionStore()
// 	rm := rootmap.NewMemRootmap()
// 	tmgr := treemgr.NewTreeManager(ns, vs, rm, 2)
//
// 	f, err := os.Open("/home/wyatt/data/bags/demo.mcap")
// 	require.NoError(t, err)
// 	defer f.Close()
//
// 	require.NoError(t, tmgr.Receive(ctx, "my-device", f))
// 	require.NoError(t, tmgr.SyncWAL(ctx))
// }
//
// func TestTreeMgr(t *testing.T) {
// 	ctx := context.Background()
// 	store := storage.NewMemStore()
// 	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
// 	db, err := sql.Open("sqlite3", ":memory:")
// 	require.NoError(t, err)
// 	wal, err := nodestore.NewSQLWAL(ctx, db)
// 	require.NoError(t, err)
// 	ns := nodestore.NewNodestore(store, cache, wal)
// 	vs := versionstore.NewMemVersionStore()
// 	rm := rootmap.NewMemRootmap()
// 	tmgr := treemgr.NewTreeManager(ns, vs, rm, 2)
//
// 	nodeID, err := ns.NewRoot(ctx, 0, 4096, 64, 64)
// 	require.NoError(t, err)
// 	version, err := vs.Next(ctx)
// 	require.NoError(t, err)
// 	require.NoError(t, rm.Put(ctx, "stream", version, nodeID))
//
// 	buf := &bytes.Buffer{}
// 	mcap.WriteFile(t, buf, []uint64{10})
// 	err = tmgr.Insert(ctx, "stream", 10, buf.Bytes())
// 	require.NoError(t, err)
//
// 	err = tmgr.SyncWAL(ctx) // noop
// 	require.NoError(t, err)
//
// 	buf2 := &bytes.Buffer{}
// 	mcap.WriteFile(t, buf2, []uint64{90})
// 	err = tmgr.Insert(ctx, "stream", 70, buf2.Bytes())
// 	require.NoError(t, err)
//
// 	err = tmgr.SyncWAL(ctx) // noop
// 	require.NoError(t, err, "failed to sync wal")
// }
//
