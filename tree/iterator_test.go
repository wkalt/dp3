package tree_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	fm "github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
)

func TestTreeIterator(t *testing.T) {
	ctx := context.Background()
	ns := nodestore.MockNodestore(ctx, t)
	rootID, err := ns.NewRoot(ctx, 0, util.Pow(uint64(64), 3), 64, 64)
	require.NoError(t, err)

	// create two mcap files and stick them into the tree
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}
	offset := 0
	for _, buf := range []*bytes.Buffer{buf1, buf2} {
		w, err := mcap.NewWriter(buf)
		require.NoError(t, err)
		require.NoError(t, w.WriteHeader(&fm.Header{}))
		require.NoError(t, w.WriteSchema(&fm.Schema{
			ID:       1,
			Name:     "schema1",
			Encoding: "ros1",
			Data:     []byte{0x01, 0x02, 0x03},
		}))
		require.NoError(t, w.WriteChannel(&fm.Channel{
			ID:              0,
			SchemaID:        1,
			Topic:           "/topic",
			MessageEncoding: "ros1msg",
		}))
		for i := 0; i < 10; i++ {
			require.NoError(t, w.WriteMessage(&fm.Message{
				LogTime: uint64(i + offset),
				Data:    []byte("hello"),
			}))
		}
		require.NoError(t, w.Close())
		offset += 64
	}
	node, err := ns.Get(ctx, rootID)
	require.NoError(t, err)
	fmt.Printf("NODE0 IS %T\n", node)
	fmt.Println(tree.PrintTree(ctx, ns, rootID, 0))
	version := uint64(1)
	rootID, _, err = tree.Insert(ctx, ns, rootID, version, 0, buf1.Bytes())
	require.NoError(t, err)
	node, err = ns.Get(ctx, rootID)
	require.NoError(t, err)
	fmt.Printf("NODE1 IS %T\n", node)
	fmt.Println(tree.PrintTree(ctx, ns, rootID, 0))
	version++
	rootID, _, err = tree.Insert(ctx, ns, rootID, version, 64*1e9, buf2.Bytes())
	require.NoError(t, err)
	version++
	fmt.Println(tree.PrintTree(ctx, ns, rootID, 0))

	node, err = ns.Get(ctx, rootID)
	require.NoError(t, err)
	fmt.Printf("NODE2 IS %T\n", node)

	it, err := tree.NewTreeIterator(ctx, ns, rootID, 0, 128)
	require.NoError(t, err)
	count := 0
	for it.More() {
		schema, channel, message, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), message.Data)
		require.Equal(t, "schema1", schema.Name)
		require.Equal(t, "/topic", channel.Topic)
		count++
	}
	assert.Equal(t, 20, count)
	t.Fail()
}
