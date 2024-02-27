package nodestore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
)

func TestInnerNode(t *testing.T) {
	node := nodestore.NewInnerNode(10, 20, 1, 3)
	t.Run("serialization", func(t *testing.T) {
		bytes, err := node.ToBytes()
		require.NoError(t, err)
		assert.Equal(t, `{"version":1,"start":10,"end":20,"children":[0,0,0]}`, string(bytes))
	})
	t.Run("deserialization", func(t *testing.T) {
		data := []byte(`{"version":1,"start":10,"end":20,"children":[0,0,0]}`)
		node := nodestore.NewInnerNode(0, 0, 0, 0)
		err := node.FromBytes(data)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), node.Version)
		assert.Equal(t, uint64(10), node.Start)
		assert.Equal(t, uint64(20), node.End)
		assert.Equal(t, []uint64{0, 0, 0}, node.Children)
	})
	assert.Equal(t, nodestore.Inner, node.Type())
}
