package nodestore_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
)

func genNodeID(t *testing.T) nodestore.NodeID {
	t.Helper()
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	require.NoError(t, err)
	return nodestore.NodeID(bytes)
}

func TestSQLWAL(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	wal, err := nodestore.NewSQLWAL(ctx, db)
	require.NoError(t, err)

	producer := "my-device"
	topic := "my-topic"

	t.Run("test get", func(t *testing.T) {
		nodeID := genNodeID(t)
		entry := nodestore.WALEntry{
			ProducerID: producer,
			Topic:      topic,
			NodeID:     nodeID,
			Version:    10,
			Data:       []byte("data"),
		}
		err := wal.Put(ctx, entry)
		require.NoError(t, err)
		_, err = wal.Get(ctx, nodeID)
		require.NoError(t, err)
	})
}
