package nodestore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
)

func pfix(prefix uint8, s string) []byte {
	return append([]byte{prefix}, []byte(s)...)
}

func TestInnerNode(t *testing.T) {
	node := nodestore.NewInnerNode(0, 10, 20, 3)
	t.Run("serialization", func(t *testing.T) {
		bytes := node.ToBytes()
		expected := pfix(
			1, `{"start":10,"end":20,"height":0,"children":[null,null,null]}`)
		assert.Equal(t, expected, bytes)
	})
	t.Run("deserialization", func(t *testing.T) {
		data := pfix(
			1, `{"start":10,"end":20,"height":0,"children":[null,null,null]}`)
		node := nodestore.NewInnerNode(0, 0, 0, 0)
		err := node.FromBytes(data)
		require.NoError(t, err)
		assert.Equal(t, uint64(10), node.Start)
		assert.Equal(t, uint64(20), node.End)
		assert.Equal(t,
			[]*nodestore.Child{nil, nil, nil},
			node.Children,
		)
	})
	t.Run("deserializing a leaf node as an inner node", func(t *testing.T) {
		data := pfix(128, `{"start":10,"end":20,"data":"hello"}`)
		node := nodestore.NewInnerNode(0, 0, 0, 0)
		assert.Error(t, node.FromBytes(data))
	})
	t.Run("deserializing a corrupted inner node", func(t *testing.T) {
		data := pfix(1, `{"start":10,"end":20`)
		node := nodestore.NewInnerNode(0, 0, 0, 0)
		err := node.FromBytes(data)
		assert.Error(t, err)
	})
	assert.Equal(t, nodestore.Inner, node.Type())
}
