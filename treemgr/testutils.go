package treemgr

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/versionstore"
)

func TestTreeManager(ctx context.Context, tb testing.TB) (*TreeManager, func()) {
	tb.Helper()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
	ns := nodestore.NewNodestore(store, cache)
	vs := versionstore.NewMemVersionStore()
	rm := rootmap.NewMemRootmap()
	tmpdir, err := os.MkdirTemp("", "dp3-test")
	require.NoError(tb, err)
	tmgr, err := NewTreeManager(
		ctx,
		ns,
		vs,
		rm,
		WithWALBufferSize(100),
		WithSyncWorkers(0), // control syncing manually
		WithWALDir(tmpdir),
	)
	require.NoError(tb, err)
	return tmgr, func() {
		os.RemoveAll(tmpdir)
	}
}
