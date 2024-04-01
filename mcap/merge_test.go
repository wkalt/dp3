package mcap_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/util/testutils"
)

func TestMerge(t *testing.T) {
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}

	mcap.WriteFile(t, buf1, []int64{0, 1, 20, 30})
	mcap.WriteFile(t, buf2, []int64{10, 11, 22, 35})

	require.NoError(t, os.WriteFile("a.mcap", buf1.Bytes(), 0600))

	buf3 := &bytes.Buffer{}
	require.NoError(t, mcap.Merge(buf3, buf1, buf2))

	reader, err := mcap.NewReader(bytes.NewReader(buf3.Bytes()))
	require.NoError(t, err)

	info, err := reader.Info()
	require.NoError(t, err)
	assert.Equal(t, uint64(8), info.Statistics.MessageCount)
	assert.Equal(t, uint64(0), info.Statistics.MessageStartTime)
	assert.Equal(t, uint64(35), info.Statistics.MessageEndTime)

	msgs, err := reader.Messages()
	require.NoError(t, err)
	n := uint64(0)
	for {
		_, _, msg, err := msgs.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		assert.Equal(t, "hello", testutils.ReadPrefixedString(t, msg.Data))
		assert.GreaterOrEqual(t, msg.LogTime, n)
		n = msg.LogTime
	}
}

func TestIdenticalSchemas(t *testing.T) {
	t.Run("schemas differ only in comments", func(t *testing.T) {
		buf1 := &bytes.Buffer{}
		buf2 := &bytes.Buffer{}
		iterators := make([]fmcap.MessageIterator, 2)
		for i, buf := range []*bytes.Buffer{buf1, buf2} {
			writer, err := mcap.NewWriter(buf)
			require.NoError(t, err)
			require.NoError(t, writer.WriteHeader(&fmcap.Header{}))
			require.NoError(t, writer.WriteSchema(&fmcap.Schema{
				ID:       1,
				Name:     "pkg/Type",
				Encoding: "ros1msg",
				Data:     []byte(fmt.Sprintf("string data # %d", i)),
			}))
			require.NoError(t, writer.WriteChannel(&fmcap.Channel{
				ID:              0,
				SchemaID:        1,
				Topic:           "/foo",
				MessageEncoding: "ros1msg",
			}))
			require.NoError(t, writer.WriteMessage(&fmcap.Message{
				ChannelID: 0,
				LogTime:   100,
				Data:      testutils.PrefixedString("hello"),
			}))
			require.NoError(t, writer.Close())
			reader, err := mcap.NewReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			iterators[i], err = reader.Messages()
			require.NoError(t, err)
		}
		output := &bytes.Buffer{}
		require.NoError(t, mcap.Nmerge(output, iterators...))
		times := mcap.ReadFile(t, bytes.NewReader(output.Bytes()))
		assert.Equal(t, []uint64{100, 100}, times)
	})
	t.Run("identical schemas, different channels", func(t *testing.T) {
		buf1 := &bytes.Buffer{}
		buf2 := &bytes.Buffer{}
		iterators := make([]fmcap.MessageIterator, 2)
		for i, buf := range []*bytes.Buffer{buf1, buf2} {
			writer, err := mcap.NewWriter(buf)
			require.NoError(t, err)
			require.NoError(t, writer.WriteHeader(&fmcap.Header{}))
			require.NoError(t, writer.WriteSchema(&fmcap.Schema{
				ID:       uint16(5*i + 1),
				Name:     "pkg/Type",
				Encoding: "ros1msg",
				Data:     []byte("string data"),
			}))
			require.NoError(t, writer.WriteChannel(&fmcap.Channel{
				ID:              uint16(5 * i),
				SchemaID:        uint16(5*i + 1),
				Topic:           "/foo",
				MessageEncoding: "ros1msg",
				Metadata:        map[string]string{"foo": strconv.Itoa(i)},
			}))
			require.NoError(t, writer.WriteMessage(&fmcap.Message{
				ChannelID: uint16(5 * i),
				LogTime:   100,
				Data:      testutils.PrefixedString("hello"),
			}))
			require.NoError(t, writer.Close())
			reader, err := mcap.NewReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			iterators[i], err = reader.Messages()
			require.NoError(t, err)
		}
		output := &bytes.Buffer{}
		require.NoError(t, mcap.Nmerge(output, iterators...))
		times := mcap.ReadFile(t, bytes.NewReader(output.Bytes()))
		assert.Equal(t, []uint64{100, 100}, times)
	})
	t.Run("schemas differ in content", func(t *testing.T) {
		buf1 := &bytes.Buffer{}
		buf2 := &bytes.Buffer{}
		iterators := make([]fmcap.MessageIterator, 2)
		for i, buf := range []*bytes.Buffer{buf1, buf2} {
			writer, err := mcap.NewWriter(buf)
			require.NoError(t, err)
			require.NoError(t, writer.WriteHeader(&fmcap.Header{}))
			schema := ""
			data := []byte{}
			for j := 0; j < i+1; j++ {
				schema += fmt.Sprintf("string data%d\n", i)
				data = append(data, testutils.PrefixedString("hello")...)
			}
			require.NoError(t, writer.WriteSchema(&fmcap.Schema{
				ID:       1,
				Name:     "pkg/Type",
				Encoding: "ros1msg",
				Data:     []byte(schema),
			}))
			require.NoError(t, writer.WriteChannel(&fmcap.Channel{
				ID:              0,
				SchemaID:        1,
				Topic:           "/foo",
				MessageEncoding: "ros1msg",
			}))
			require.NoError(t, writer.WriteMessage(&fmcap.Message{
				ChannelID: 0,
				LogTime:   100,
				Data:      data,
			}))
			require.NoError(t, writer.Close())
			reader, err := mcap.NewReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			iterators[i], err = reader.Messages()
			require.NoError(t, err)
		}
		output := &bytes.Buffer{}
		require.NoError(t, mcap.Nmerge(output, iterators...))
		times := mcap.ReadFile(t, bytes.NewReader(output.Bytes()))
		assert.Equal(t, []uint64{100, 100}, times)
	})
	t.Run("identical schemas different channel metadata", func(t *testing.T) {
		buf1 := &bytes.Buffer{}
		buf2 := &bytes.Buffer{}
		iterators := make([]fmcap.MessageIterator, 2)
		for i, buf := range []*bytes.Buffer{buf1, buf2} {
			writer, err := mcap.NewWriter(buf)
			require.NoError(t, err)
			require.NoError(t, writer.WriteHeader(&fmcap.Header{}))
			schema := ""
			data := []byte{}
			for j := 0; j < i+1; j++ {
				schema += fmt.Sprintf("string data%d\n", i)
				data = append(data, testutils.PrefixedString("hello")...)
			}
			require.NoError(t, writer.WriteSchema(&fmcap.Schema{
				ID:       uint16(i + 1),
				Name:     "pkg/Type",
				Encoding: "ros1msg",
				Data:     []byte(schema),
			}))
			require.NoError(t, writer.WriteChannel(&fmcap.Channel{
				ID:              uint16(i),
				SchemaID:        uint16(i + 1),
				Topic:           "/foo",
				MessageEncoding: "ros1msg",
				Metadata: map[string]string{
					"foo": strconv.Itoa(i),
				},
			}))
			require.NoError(t, writer.WriteMessage(&fmcap.Message{
				ChannelID: uint16(i),
				LogTime:   100,
				Data:      data,
			}))
			require.NoError(t, writer.Close())
			reader, err := mcap.NewReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			iterators[i], err = reader.Messages()
			require.NoError(t, err)
		}
		output := &bytes.Buffer{}
		require.NoError(t, mcap.Nmerge(output, iterators...))
		times := mcap.ReadFile(t, bytes.NewReader(output.Bytes()))
		assert.Equal(t, []uint64{100, 100}, times)
	})
}

func TestNMerge(t *testing.T) {
	cases := []struct {
		assertion string
		input     [][]int64
		messages  []int
	}{
		{
			"single-topic file, single message",
			[][]int64{{10}},
			[]int{10},
		},
		{
			"single-topic file, multiple messages",
			[][]int64{{10, 20, 30}},
			[]int{10, 20, 30},
		},
		{
			"multiple files, single message",
			[][]int64{{10}, {20}, {30}},
			[]int{10, 20, 30},
		},
		{
			"multiple files, multiple messages",
			[][]int64{{10, 20, 30}, {40, 50, 60}, {70, 80, 90}},
			[]int{10, 20, 30, 40, 50, 60, 70, 80, 90},
		},
		{
			"multiple files, overlapping",
			[][]int64{{10, 20, 30}, {21, 31, 41}, {32, 42, 52}},
			[]int{10, 20, 21, 30, 31, 32, 41, 42, 52},
		},
		{
			"multiple files, including empty file",
			[][]int64{{10, 20, 30}, {}, {40, 50, 60}},
			[]int{10, 20, 30, 40, 50, 60},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			bufs := make([]*bytes.Buffer, len(c.input))
			for i, timestamps := range c.input {
				bufs[i] = &bytes.Buffer{}
				mcap.WriteFile(t, bufs[i], timestamps)
			}
			iterators := make([]fmcap.MessageIterator, len(bufs))
			for i, buf := range bufs {
				reader, err := mcap.NewReader(bytes.NewReader(buf.Bytes()))
				require.NoError(t, err)
				iterators[i], err = reader.Messages()
				require.NoError(t, err)
			}
			output := &bytes.Buffer{}
			require.NoError(t, mcap.Nmerge(output, iterators...))
			reader, err := mcap.NewReader(bytes.NewReader(output.Bytes()))
			require.NoError(t, err)
			messages := []int{}

			iterator, err := reader.Messages()
			require.NoError(t, err)
			for {
				_, _, msg, err := iterator.Next(nil)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				messages = append(messages, int(msg.LogTime))
			}
			require.Equal(t, c.messages, messages)
		})
	}
}
