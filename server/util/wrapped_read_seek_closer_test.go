package util_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/util"
)

func TestInsetReadSeekCloser(t *testing.T) {
	data := []byte("hello world")

	newrsc := func(inset int64) io.ReadSeekCloser {
		rsc := util.NewReadSeekNopCloser(bytes.NewReader(data))
		_, err := rsc.Seek(inset, io.SeekStart)
		require.NoError(t, err)
		return rsc
	}

	t.Run("read all", func(t *testing.T) {
		inset := int64(1)
		rsc := newrsc(inset)
		wrapped := util.NewWrappedReadSeekCloser(rsc, inset, 4)
		output, err := io.ReadAll(wrapped)
		require.NoError(t, err)
		require.Equal(t, []byte("ello"), output)
	})

	t.Run("seeking", func(t *testing.T) {
		t.Run("seek start", func(t *testing.T) {
			inset := int64(1)
			rsc := newrsc(inset)

			wrapped := util.NewWrappedReadSeekCloser(rsc, inset, 4)
			_, err := wrapped.Seek(1, io.SeekStart)
			require.NoError(t, err)

			output, err := io.ReadAll(wrapped)
			require.NoError(t, err)
			require.Equal(t, []byte("llo"), output)
		})

		t.Run("seek current positive", func(t *testing.T) {
			inset := int64(1)
			rsc := newrsc(inset)

			wrapped := util.NewWrappedReadSeekCloser(rsc, inset, 4)
			_, err := wrapped.Seek(1, io.SeekCurrent)
			require.NoError(t, err)

			output, err := io.ReadAll(wrapped)
			require.NoError(t, err)
			require.Equal(t, []byte("llo"), output)
		})
		t.Run("seek current negative", func(t *testing.T) {
			inset := int64(1)
			rsc := newrsc(inset)

			wrapped := util.NewWrappedReadSeekCloser(rsc, inset, 4)
			_, err := wrapped.Seek(2, io.SeekStart)
			require.NoError(t, err)

			_, err = wrapped.Seek(-1, io.SeekCurrent)
			require.NoError(t, err)

			output, err := io.ReadAll(wrapped)
			require.NoError(t, err)
			require.Equal(t, []byte("llo"), output)
		})
		t.Run("seek end", func(t *testing.T) {
			inset := int64(1)
			rsc := newrsc(inset)

			wrapped := util.NewWrappedReadSeekCloser(rsc, inset, 4)

			_, err := wrapped.Seek(-1, io.SeekEnd)
			require.NoError(t, err)

			output, err := io.ReadAll(wrapped)
			require.NoError(t, err)
			require.Equal(t, []byte("o"), output)
		})
	})
}
