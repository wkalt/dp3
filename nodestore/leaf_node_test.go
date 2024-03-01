package nodestore

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
)

func TestLeafNode(t *testing.T) {
	buf := &bytes.Buffer{}
	mcap.WriteFile(t, buf, []uint64{1, 2, 3})
	t.Run("serialization", func(t *testing.T) {
		node := NewLeafNode(buf.Bytes())
		expected := append([]byte{leafNodeVersion + 128}, buf.Bytes()...)
		assert.Equal(t, expected, node.ToBytes())
	})
	t.Run("deserialization", func(t *testing.T) {
		node := NewLeafNode([]byte{})
		err := node.FromBytes(append([]byte{leafNodeVersion + 128}, buf.Bytes()...))
		assert.NoError(t, err)
		assert.Equal(t, buf.Bytes(), node.data)
	})
	t.Run("deserializing an inner node as a leaf node triggers error", func(t *testing.T) {
		node := NewLeafNode([]byte{})
		data := append([]byte{innerNodeVersion}, buf.Bytes()...)
		require.Error(t, node.FromBytes(data))
	})
	t.Run("merging leaf nodes", func(t *testing.T) {
		buf1 := &bytes.Buffer{}
		mcap.WriteFile(t, buf1, []uint64{1, 2, 10})
		node1 := NewLeafNode(buf1.Bytes())
		buf2 := &bytes.Buffer{}
		mcap.WriteFile(t, buf2, []uint64{4, 5, 6})
		node2, err := node1.Merge(buf2.Bytes())
		require.NoError(t, err)
		assert.Equal(t, []uint64{1, 2, 4, 5, 6, 10}, mcap.ReadFile(t, node2.Data()))
	})
	t.Run("merging garbage", func(t *testing.T) {
		buf1 := &bytes.Buffer{}
		mcap.WriteFile(t, buf1, []uint64{1, 2, 10})
		node1 := NewLeafNode(buf1.Bytes())
		buf2 := &bytes.Buffer{}
		mcap.WriteFile(t, buf2, []uint64{4, 5, 6})
		_, err := node1.Merge([]byte{0, 1, 2, 3, 4})
		require.Error(t, err)
	})
	t.Run("type", func(t *testing.T) {
		node := NewLeafNode([]byte{})
		assert.Equal(t, Leaf, node.Type())
	})
	t.Run("string", func(t *testing.T) {
		node := NewLeafNode([]byte{})
		assert.Equal(t, "[leaf 0]", node.String())
	})
}
