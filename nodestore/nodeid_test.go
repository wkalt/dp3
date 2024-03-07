package nodestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeIDScanner(t *testing.T) {
	id := generateNodeID(objectID(9223372036854775807), 2377854190, 1653944042)
	value, err := id.Value()
	require.NoError(t, err)
	var id2 NodeID
	require.NoError(t, id2.Scan(value))
	assert.Equal(t, id, id2)
}
