package treemgr

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

func TestTreeManager(ctx context.Context, tb testing.TB) (*TreeManager, func()) {
	tb.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	require.NoError(tb, err)

	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
	ns := nodestore.NewNodestore(store, cache)

	rm, err := rootmap.NewSQLRootmap(ctx, db, rootmap.WithReservationSize(1e9))
	require.NoError(tb, err)

	tmpdir, err := os.MkdirTemp("", "dp3-test")
	require.NoError(tb, err)
	tmgr, err := NewTreeManager(
		ctx,
		ns,
		rm,
		WithWALBufferSize(100),
		WithSyncWorkers(0), // control syncing manually
		WithWALDir(tmpdir),
	)
	require.NoError(tb, err)
	return tmgr, func() {
		os.RemoveAll(tmpdir)
		db.Close()
	}
}
