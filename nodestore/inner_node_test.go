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
	node := nodestore.NewInnerNode(10, 20, 3)
	t.Run("serialization", func(t *testing.T) {
		bytes, err := node.ToBytes()
		require.NoError(t, err)
		expected := pfix(
			1, `{"start":10,"end":20,"children":[null,null,null]}`)
		assert.Equal(t, expected, bytes)
	})
	t.Run("deserialization", func(t *testing.T) {
		data := pfix(
			1, `{"start":10,"end":20,"children":[null,null,null]}`)
		node := nodestore.NewInnerNode(0, 0, 0)
		err := node.FromBytes(data)
		require.NoError(t, err)
		assert.Equal(t, uint64(10), node.Start)
		assert.Equal(t, uint64(20), node.End)
		assert.Equal(t,
			[]*nodestore.Child{nil, nil, nil},
			node.Children,
		)
	})
	assert.Equal(t, nodestore.Inner, node.Type())
}
