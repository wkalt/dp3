package mcap

import (
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/require"
)

func WriteFile(t *testing.T, w io.Writer, timestamps []uint64) {
	writer, err := NewWriter(w)
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&mcap.Header{}))

	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID:   1,
		Data: []byte{},
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:       0,
		SchemaID: 1,
		Topic:    "/foo",
	}))

	for _, ts := range timestamps {
		require.NoError(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 0,
			LogTime:   ts,
			Data:      []byte("hello"),
		}))
	}
	require.NoError(t, writer.Close())
}
