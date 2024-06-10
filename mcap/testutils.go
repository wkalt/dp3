package mcap

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/testutils"
)

func ReadFile(t *testing.T, r io.Reader) []uint64 {
	t.Helper()
	reader, err := NewReader(r)
	require.NoError(t, err)

	msgs, err := reader.Messages()
	require.NoError(t, err)

	var timestamps []uint64
	for {
		schema, _, msg, err := msgs.NextInto(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.NotNil(t, schema, "got a nil schema")
		timestamps = append(timestamps, msg.LogTime)
	}
	return timestamps
}

func WriteFileExtended(t *testing.T, w io.Writer, fieldCount int, timestampsets ...[]int64) {
	t.Helper()
	writer, err := NewWriter(w)
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&mcap.Header{}))

	schema := ""
	for i := range fieldCount {
		schema += fmt.Sprintf("string data%d\n", i)
	}
	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "test",
		Encoding: "ros1msg",
		Data:     []byte(schema),
	}))
	for i := range timestampsets {
		require.NoError(t, writer.WriteChannel(&mcap.Channel{
			ID:       uint16(i),
			SchemaID: 1,
			Topic:    fmt.Sprintf("topic-%d", i),
		}))
	}
	for chanID, timestamps := range timestampsets {
		for _, ts := range timestamps {
			data := []byte{}
			for i := 0; i < fieldCount; i++ {
				data = append(data, testutils.U32b(5)...)
				data = append(data, []byte("hello")...)
			}
			require.NoError(t, writer.WriteMessage(&mcap.Message{
				ChannelID: uint16(chanID),
				LogTime:   uint64(ts),
				Data:      data,
			}))
		}
	}
	require.NoError(t, writer.Close())
}

func WriteFile(t *testing.T, w io.Writer, timestampsets ...[]int64) {
	t.Helper()
	writer, err := NewWriter(w)
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&mcap.Header{}))

	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "test",
		Encoding: "ros1msg",
		Data: []byte(`
		string data
		int16 count
		`),
	}))
	for i := range timestampsets {
		channel := NewChannel(uint16(i), 1, fmt.Sprintf("topic-%d", i), "ros1msg", nil)
		require.NoError(t, writer.WriteChannel(channel))
	}
	for chanID, timestamps := range timestampsets {
		for _, ts := range timestamps {
			require.NoError(t, writer.WriteMessage(&mcap.Message{
				ChannelID: uint16(chanID),
				LogTime:   uint64(ts),
				Data:      testutils.Flatten(testutils.U32b(5), []byte("hello"), testutils.U16b(2024)),
			}))
		}
	}
	require.NoError(t, writer.Close())
}
