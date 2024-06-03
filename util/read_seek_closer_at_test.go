package util_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util"
)

func TestReadSeekCloserAt(t *testing.T) {
	r := util.NewReadSeekNopCloser(bytes.NewReader([]byte("!!hello world!!")))

	t.Run("read", func(t *testing.T) {
		rsc, err := util.NewReadSeekCloserAt(r, 2, 11)
		require.NoError(t, err)

		buf := make([]byte, 5)
		n, err := rsc.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, "hello", string(buf))

		bytes, err := io.ReadAll(rsc)
		require.NoError(t, err)
		require.Equal(t, " world", string(bytes))
	})

	t.Run("seek", func(t *testing.T) {
		rsc, err := util.NewReadSeekCloserAt(r, 2, 11)
		require.NoError(t, err)

		t.Run("seek start", func(t *testing.T) {
			_, err = rsc.Seek(2, io.SeekStart)
			require.NoError(t, err)
			bytes, err := io.ReadAll(rsc)
			require.NoError(t, err)
			require.Equal(t, "llo world", string(bytes))
		})
		t.Run("seek current", func(t *testing.T) {
			_, err = rsc.Seek(2, io.SeekStart)
			require.NoError(t, err)
			_, err = rsc.Seek(2, io.SeekCurrent)
			require.NoError(t, err)
			bytes, err := io.ReadAll(rsc)
			require.NoError(t, err)
			require.Equal(t, "o world", string(bytes))
		})
		t.Run("seek end", func(t *testing.T) {
			_, err = rsc.Seek(-2, io.SeekEnd)
			require.NoError(t, err)
			bytes, err := io.ReadAll(rsc)
			require.NoError(t, err)
			require.Equal(t, "ld", string(bytes))
		})
	})
}
