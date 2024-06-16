package nodestore_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

func TestNodestore(t *testing.T) {
	var prefix = "prefix"
	t.Run("Put", func(t *testing.T) {
		ctx := context.Background()
		store := storage.NewMemStore()
		cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1024)
		ns := nodestore.NewNodestore(store, cache)
		node := nodestore.NewLeafNode(nil, []byte("test"), nil, nil)
		data := node.ToBytes()
		require.NoError(t, ns.Put(ctx, prefix, 1, bytes.NewReader(data)))
	})

	t.Run("Get", func(t *testing.T) {
		ctx := context.Background()
		buf := &bytes.Buffer{}
		n, err := buf.Write(make([]byte, 1000))
		require.NoError(t, err)

		store := storage.NewMemStore()
		cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1024)
		ns := nodestore.NewNodestore(store, cache)

		data := []byte("hello")
		node := nodestore.NewLeafNode(nil, data, nil, nil)
		bytes := node.ToBytes()
		_, err = buf.Write(bytes)
		require.NoError(t, err)
		addr := nodestore.NewNodeID(1, uint64(n), uint64(len(bytes)))

		require.NoError(t, ns.Put(ctx, prefix, 1, buf))

		retrieved, err := ns.Get(ctx, prefix, addr)
		require.NoError(t, err)

		leaf, ok := retrieved.(*nodestore.LeafNode)
		require.True(t, ok)

		found, err := io.ReadAll(leaf.Data())
		require.NoError(t, err)
		require.Equal(t, data, found)

		// hits the cache
		_, err = ns.Get(ctx, prefix, addr)
		require.NoError(t, err)
	})
}
