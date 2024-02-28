package nodestore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
)

func TestLeafNode(t *testing.T) {
	node := nodestore.NewLeafNode([]nodestore.Record{{1, []byte("a")}})
	t.Run("serialization", func(t *testing.T) {
		bytes, err := node.ToBytes()
		require.NoError(t, err)
		assert.Equal(t, `{"records":[{"time":1,"data":"YQ=="}]}`, string(bytes))
	})
	t.Run("deserialization", func(t *testing.T) {
		data := []byte(`{"records":[{"time":1,"data":"YQ=="}]}`)
		err := node.FromBytes(data)
		require.NoError(t, err)
		assert.Equal(t, []nodestore.Record{{1, []byte("a")}}, node.Records)
	})
	assert.Equal(t, nodestore.Leaf, node.Type())
}
