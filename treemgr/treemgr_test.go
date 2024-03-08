package treemgr_test

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestGetMessages(t *testing.T) {
}

func TestGetStatistics(t *testing.T) {
}

func TestInsert(t *testing.T) {
}

func TestGetMessagesLatest(t *testing.T) {
}

func TestGetStatisticsLatest(t *testing.T) {
}

func TestSyncWAL(t *testing.T) {
}

// func TestReceive(t *testing.T) {
// 	ctx := context.Background()
// 	buf := &bytes.Buffer{}
//
// 	cases := []struct {
// 		assertion string
// 		input     [][]uint64
// 		output    string
// 	}{
// 		{
// 			"single-topic file, single message",
// 			[][]uint64{{10}},
// 			"",
// 		},
// 	}
// 	for _, c := range cases {
// 		t.Run(c.assertion, func(t *testing.T) {
// 			buf.Reset()
// 			mcap.WriteFile(t, buf, c.input...)
// 			tmgr := testTreeManager(t)
// 			require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
//
// 			str := tmgr.printStream(ctx, "my-device")
// 			require.Equal(t, c.output, "")
// 		})
// 	}
//
// 	mcap.WriteFile(t, buf, []uint64{10, 100, 1000, 10000})
// 	tmgr := testTreeManager(t)
// 	require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
// }
//
// func testTreeManager(t *testing.T) *treemgr.TreeManager {
// 	t.Helper()
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
// 	return tmgr
// }

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
