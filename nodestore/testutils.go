package nodestore

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

func MockNodestore(ctx context.Context, t *testing.T) *Nodestore {
	store := storage.NewMemStore()
	cache := util.NewLRU[NodeID, Node](1e6)
	wal := NewMemWAL()
	return NewNodestore(store, cache, wal)
}
