package treemgr

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/versionstore"
)

func TestIngestion(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	wal, err := nodestore.NewSQLWAL(ctx, db)
	require.NoError(t, err)
	ns := nodestore.NewNodestore(store, cache, wal)
	vs := versionstore.NewMemVersionStore()
	rm := rootmap.NewMemRootmap()
	tmgr := NewTreeManager(ns, vs, rm, 2)

	f, err := os.Open("/home/wyatt/data/bags/cal_loop.mcap")
	require.NoError(t, err)
	defer f.Close()

	require.NoError(t, tmgr.IngestStream(ctx, "my-device", f))
	require.NoError(t, tmgr.SyncWAL(ctx))
	fmt.Println("yodawk")
}

func TestTreeMgr(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	wal, err := nodestore.NewSQLWAL(ctx, db)
	require.NoError(t, err)
	ns := nodestore.NewNodestore(store, cache, wal)
	vs := versionstore.NewMemVersionStore()
	rm := rootmap.NewMemRootmap()
	tmgr := NewTreeManager(ns, vs, rm, 2)

	nodeID, err := ns.NewRoot(ctx, 0, 4096, 64, 64)
	require.NoError(t, err)
	version, err := vs.Next(ctx)
	require.NoError(t, err)
	rm.Put(ctx, "stream", version, nodeID)

	buf := &bytes.Buffer{}
	mcap.WriteFile(t, buf, []uint64{10})
	err = tmgr.Insert(ctx, "stream", 10, buf.Bytes())
	require.NoError(t, err)

	err = tmgr.SyncWAL(ctx) // noop
	require.NoError(t, err)

	buf2 := &bytes.Buffer{}
	mcap.WriteFile(t, buf2, []uint64{90})
	err = tmgr.Insert(ctx, "stream", 70, buf2.Bytes())
	require.NoError(t, err)

	err = tmgr.SyncWAL(ctx) // noop
	require.NoError(t, err, "failed to sync wal")
}
