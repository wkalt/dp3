package mcap

import (
	"errors"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/require"
)

func ReadFile(t *testing.T, r io.Reader) []uint64 {
	t.Helper()
	reader, err := NewReader(r)
	require.NoError(t, err)

	msgs, err := reader.Messages()
	require.NoError(t, err)

	var timestamps []uint64
	for {
		_, _, msg, err := msgs.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		timestamps = append(timestamps, msg.LogTime)
	}
	return timestamps
}

func WriteFile(t *testing.T, w io.Writer, timestamps []uint64) {
	t.Helper()
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
