package rootmap_test

import (
	"crypto/rand"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
)

func TestSQLRootmap(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	rm, err := rootmap.NewSQLRootmap(db)
	require.NoError(t, err)

	t.Run("put", func(t *testing.T) {
		streamID := uuid.New()
		expected := randNodeID()
		err := rm.Put(streamID, 10, expected)
		require.NoError(t, err)

		nodeID, err := rm.Get(streamID, 10)
		require.NoError(t, err)
		require.Equal(t, expected, nodeID)
	})

	t.Run("get latest", func(t *testing.T) {
		streamID := uuid.New()
		node1 := randNodeID()
		err := rm.Put(streamID, 10, node1)
		require.NoError(t, err)

		node2 := randNodeID()
		err = rm.Put(streamID, 20, node2)
		require.NoError(t, err)

		nodeID, err := rm.GetLatest(streamID)
		require.NoError(t, err)
		require.Equal(t, node2, nodeID)
	})
}

func randNodeID() nodestore.NodeID {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	var id nodestore.NodeID
	copy(id[:], buf)
	return id
}
