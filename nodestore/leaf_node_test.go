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
		testutils.U64b(0),
		buf,
	)
}

func TestLeafNode(t *testing.T) {
	buf := &bytes.Buffer{}
	mcap.WriteFile(t, buf, []int64{1, 2, 3})
	t.Run("serialization", func(t *testing.T) {
		node := nodestore.NewLeafNode(nil, buf.Bytes(), nil, nil)
		expected := leafNodeBytes(buf.Bytes())
		assert.Equal(t, expected, node.ToBytes())
	})
	t.Run("deserialization", func(t *testing.T) {
		node := nodestore.NewLeafNode(nil, []byte{}, nil, nil)
		err := node.FromBytes(leafNodeBytes(buf.Bytes()))
		require.NoError(t, err)
		data, err := io.ReadAll(node.Data())
		require.NoError(t, err)
		assert.Equal(t, buf.Bytes(), data)
	})
	t.Run("deserializing an inner node as a leaf node triggers error", func(t *testing.T) {
		node := nodestore.NewLeafNode(nil, []byte{}, nil, nil)
		data := append([]byte{1}, buf.Bytes()...)
		require.Error(t, node.FromBytes(data))
	})
	t.Run("type", func(t *testing.T) {
		node := nodestore.NewLeafNode(nil, []byte{}, nil, nil)
		assert.Equal(t, nodestore.Leaf, node.Type())
	})
}
