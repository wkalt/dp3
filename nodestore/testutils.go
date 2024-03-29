package nodestore

import (
	"context"
	"crypto/rand"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

func MockNodestore(ctx context.Context, t *testing.T) *Nodestore {
	t.Helper()
	store := storage.NewMemStore()
	cache := util.NewLRU[NodeID, Node](1e6)
	return NewNodestore(store, cache)
}

func RandomNodeID() NodeID {
	buf := [24]byte{}
	_, _ = rand.Read(buf[:])
	return NodeID(buf)
}
