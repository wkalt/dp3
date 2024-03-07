package nodestore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
)

func TestNodeIDScanner(t *testing.T) {
	for i := 0; i < 1e3; i++ {
		id := genNodeID(t)
		value, err := id.Value()
		require.NoError(t, err)
		var id2 nodestore.NodeID
		require.NoError(t, id2.Scan(value))
		assert.Equal(t, id, id2)
	}
}
