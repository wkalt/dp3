package nodestore_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util/testutils"
)

func leafNodeBytes(buf []byte) []byte {
	return testutils.Flatten(
		[]byte{1 + 128},
		make([]byte, 24),
		testutils.U64b(0),
		testutils.U64b(0),
		testutils.U64b(0),
		buf,
	)
}

func TestLeafNode(t *testing.T) {
	buf := &bytes.Buffer{}
	mcap.WriteFile(t, buf, []int64{1, 2, 3})
	t.Run("serialization", func(t *testing.T) {
		node := nodestore.NewLeafNode(buf.Bytes(), nil, nil)
		expected := leafNodeBytes(buf.Bytes())
		assert.Equal(t, expected, node.ToBytes())
	})
	t.Run("deserialization", func(t *testing.T) {
		node := nodestore.NewLeafNode([]byte{}, nil, nil)
		err := node.FromBytes(leafNodeBytes(buf.Bytes()))
		require.NoError(t, err)
		data, err := io.ReadAll(node.Data())
		require.NoError(t, err)
		assert.Equal(t, buf.Bytes(), data)
	})
	t.Run("deserializing an inner node as a leaf node triggers error", func(t *testing.T) {
		node := nodestore.NewLeafNode([]byte{}, nil, nil)
		data := append([]byte{1}, buf.Bytes()...)
		require.Error(t, node.FromBytes(data))
	})
	t.Run("merging leaf nodes", func(t *testing.T) {
		buf1 := &bytes.Buffer{}
		mcap.WriteFile(t, buf1, []int64{1, 2, 10})
		node1 := nodestore.NewLeafNode(buf1.Bytes(), nil, nil)
		buf2 := &bytes.Buffer{}
		mcap.WriteFile(t, buf2, []int64{4, 5, 6})
		node2, err := node1.Merge(buf2.Bytes())
		require.NoError(t, err)
		assert.Equal(t, []uint64{1, 2, 4, 5, 6, 10}, mcap.ReadFile(t, node2.Data()))
	})
	t.Run("merging garbage", func(t *testing.T) {
		buf1 := &bytes.Buffer{}
		mcap.WriteFile(t, buf1, []int64{1, 2, 10})
		node1 := nodestore.NewLeafNode(buf1.Bytes(), nil, nil)
		buf2 := &bytes.Buffer{}
		mcap.WriteFile(t, buf2, []int64{4, 5, 6})
		_, err := node1.Merge([]byte{0, 1, 2, 3, 4})
		require.Error(t, err)
	})
	t.Run("type", func(t *testing.T) {
		node := nodestore.NewLeafNode([]byte{}, nil, nil)
		assert.Equal(t, nodestore.Leaf, node.Type())
	})
}
