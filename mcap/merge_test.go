package mcap_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
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
		assert.Equal(t, []byte("hello"), msg.Data)
		assert.GreaterOrEqual(t, msg.LogTime, n)
		n = msg.LogTime
	}
}
