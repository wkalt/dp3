package mcap_test

import (
	"bytes"
	"errors"
	"io"
	"os"
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

	mcap.WriteFile(t, buf1, []uint64{0, 1, 20, 30})
	mcap.WriteFile(t, buf2, []uint64{10, 11, 22, 35})

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

func TestNMerge(t *testing.T) {
	cases := []struct {
		assertion string
		input     [][]uint64
		messages  []int
	}{
		{
			"single-topic file, single message",
			[][]uint64{{10}},
			[]int{10},
		},
		{
			"single-topic file, multiple messages",
			[][]uint64{{10, 20, 30}},
			[]int{10, 20, 30},
		},
		{
			"multiple files, single message",
			[][]uint64{{10}, {20}, {30}},
			[]int{10, 20, 30},
		},
		{
			"multiple files, multiple messages",
			[][]uint64{{10, 20, 30}, {40, 50, 60}, {70, 80, 90}},
			[]int{10, 20, 30, 40, 50, 60, 70, 80, 90},
		},
		{
			"multiple files, overlapping",
			[][]uint64{{10, 20, 30}, {21, 31, 41}, {32, 42, 52}},
			[]int{10, 20, 21, 30, 31, 32, 41, 42, 52},
		},
		{
			"multiple files, including empty file",
			[][]uint64{{10, 20, 30}, {}, {40, 50, 60}},
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
