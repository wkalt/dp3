package nodestore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
)

func TestInnerNode(t *testing.T) {
	node := nodestore.NewInnerNode(10, 20, 3)
	t.Run("serialization", func(t *testing.T) {
		bytes, err := node.ToBytes()
		require.NoError(t, err)
		expected := `{"start":10,"end":20,"children":[{"id":0,"version":0},{"id":0,"version":0},{"id":0,"version":0}]}`
		assert.Equal(t, expected, string(bytes))
	})
	t.Run("deserialization", func(t *testing.T) {
		data := []byte(`{"start":10,"end":20,"children":[{"id":0,"version":0},{"id":0,"version":0},{"id":0,"version":0}]}`)
		node := nodestore.NewInnerNode(0, 0, 0)
		err := node.FromBytes(data)
		require.NoError(t, err)
		assert.Equal(t, uint64(10), node.Start)
		assert.Equal(t, uint64(20), node.End)
		assert.Equal(t, []nodestore.Child{{0, 0}, {0, 0}, {0, 0}}, node.Children)
	})
	assert.Equal(t, nodestore.Inner, node.Type())
}
