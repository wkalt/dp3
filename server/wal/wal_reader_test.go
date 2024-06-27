package wal_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/wal"
)

func TestBadMagic(t *testing.T) {
	t.Run("unsupported major", func(t *testing.T) {
		buf := make([]byte, 8)
		copy(buf, wal.Magic)
		buf[6] = 1
		_, err := wal.NewReader(bytes.NewReader(buf))
		require.ErrorIs(t, err, wal.UnsupportedWALError{})
	})

	t.Run("unsupported minor", func(t *testing.T) {
		buf := make([]byte, 8)
		copy(buf, wal.Magic)
		buf[7] = 1
		_, err := wal.NewReader(bytes.NewReader(buf))
		require.ErrorIs(t, err, wal.UnsupportedWALError{})
	})

	t.Run("invalid magic", func(t *testing.T) {
		buf := make([]byte, 8)
		copy(buf, wal.Magic)
		buf[0] = 0
		_, err := wal.NewReader(bytes.NewReader(buf))
		require.ErrorIs(t, err, wal.ErrBadMagic)
	})

	t.Run("short magic", func(t *testing.T) {
		buf := make([]byte, 7)
		copy(buf, wal.Magic)
		_, err := wal.NewReader(bytes.NewReader(buf))
		require.ErrorIs(t, err, wal.ErrBadMagic)
	})
}

func TestWALCorruption(t *testing.T) {
	buf := &bytes.Buffer{}
	writer, err := wal.NewWriter(buf, 1, 0)
	require.NoError(t, err)
	_, _, err = writer.WriteInsert(wal.InsertRecord{
		Producer: "producer",
		Topic:    "topic",
		Data:     []byte{0x01, 0x02},
	})
	require.NoError(t, err)

	data := buf.Bytes()
	data[len(data)-8] = 0x99 // corrupt it

	reader, err := wal.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	_, _, err = reader.Next()
	require.ErrorIs(t, err, wal.CRCMismatchError{})
}

func TestWALReader(t *testing.T) {
	t.Run("insert", func(t *testing.T) {
		record := wal.InsertRecord{
			Producer: "producer",
			Topic:    "topic",
			BatchID:  "foobar",
			Addr:     wal.NewAddress(1, 8, 76),
			Data:     []byte("data"),
		}
		buf := &bytes.Buffer{}
		w, err := wal.NewWriter(buf, 1, 0)
		require.NoError(t, err)
		_, _, err = w.WriteInsert(record)
		require.NoError(t, err)
		reader, err := wal.NewReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		for {
			rectype, data, err := reader.Next()
			require.NoError(t, err)
			if rectype == wal.WALInsert {
				result := wal.ParseInsertRecord(data)
				require.Equal(t, record.Addr.String(), result.Addr.String())
				break
			}
		}
	})
	t.Run("mergeRequest", func(t *testing.T) {
		record := wal.MergeRequestRecord{
			Producer: "producer",
			Topic:    "topic",
			BatchID:  "id",
			Addrs:    []wal.Address{wal.NewAddress(1, 2, 3), wal.NewAddress(4, 5, 6)},
		}
		buf := &bytes.Buffer{}
		w, err := wal.NewWriter(buf, 1, 0)
		require.NoError(t, err)
		_, _, err = w.WriteMergeRequest(record)
		require.NoError(t, err)
		reader, err := wal.NewReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		for {
			rectype, data, err := reader.Next()
			require.NoError(t, err)
			if rectype == wal.WALMergeRequest {
				result := wal.ParseMergeRequestRecord(data)
				require.Equal(t, *result, record)
				break
			}
		}
	})
	t.Run("mergeComplete", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w, err := wal.NewWriter(buf, 1, 0)
		require.NoError(t, err)
		_, _, err = w.WriteMergeComplete(wal.MergeCompleteRecord{"id"})
		require.NoError(t, err)
		require.NoError(t, err)
		reader, err := wal.NewReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		for {
			rectype, data, err := reader.Next()
			require.NoError(t, err)
			if rectype == wal.WALMergeComplete {
				result := wal.ParseMergeCompleteRecord(data)
				require.Equal(t, &wal.MergeCompleteRecord{BatchID: "id"}, result)
				break
			}
		}
	})
}
