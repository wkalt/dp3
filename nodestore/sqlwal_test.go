package nodestore

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestSQLWAL(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	wal, err := NewSQLWAL(ctx, db)
	require.NoError(t, err)

	t.Run("test put/get stream", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			entry := WALEntry{
				StreamID: "stream",
				NodeID:   generateNodeID(10, 10, i),
				Version:  10,
				Data:     []byte("data"),
			}
			err := wal.Put(ctx, entry)
			require.NoError(t, err)
		}

		result, err := wal.GetStream(ctx, "stream")
		require.NoError(t, err)
		require.Equal(t, 1, len(result))
		require.Equal(t, 10, len(result[0]))
	})

	t.Run("test get", func(t *testing.T) {
		nodeID := generateNodeID(10, 10, 10)
		entry := WALEntry{
			StreamID: "stream",
			NodeID:   nodeID,
			Version:  10,
			Data:     []byte("data"),
		}
		err := wal.Put(ctx, entry)
		require.NoError(t, err)
		_, err = wal.Get(ctx, nodeID)
		require.NoError(t, err)
	})
}
