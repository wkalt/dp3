package util_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util"
)

func TestReadU8(t *testing.T) {
	var x uint8
	n := util.ReadU8([]byte{0x01}, &x)
	require.Equal(t, 1, n)
	require.Equal(t, uint8(0x01), x)
}

func TestReadBool(t *testing.T) {
	var x bool
	n := util.ReadBool([]byte{0x01}, &x)
	require.Equal(t, 1, n)
	require.True(t, x)

	n = util.ReadBool([]byte{0x00}, &x)
	require.Equal(t, 1, n)
	require.False(t, x)
}

func TestBool(t *testing.T) {
	buf := make([]byte, 1)
	n := util.Bool(buf, true)
	require.Equal(t, 1, n)
	require.Equal(t, []byte{0x01}, buf)

	n = util.Bool(buf, false)
	require.Equal(t, 1, n)
	require.Equal(t, []byte{0x00}, buf)
}

func TestReadU32(t *testing.T) {
	var x uint32
	n := util.ReadU32([]byte{0x01, 0x02, 0x03, 0x04}, &x)
	require.Equal(t, 4, n)
	require.Equal(t, uint32(0x04030201), x)
}

func TestReadU64(t *testing.T) {
	var x uint64
	n := util.ReadU64([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, &x)
	require.Equal(t, 8, n)
	require.Equal(t, uint64(0x0807060504030201), x)
}

func TestU8(t *testing.T) {
	buf := make([]byte, 1)
	n := util.U8(buf, 0x01)
	require.Equal(t, 1, n)
	require.Equal(t, []byte{0x01}, buf)
}

func TestU32(t *testing.T) {
	buf := make([]byte, 4)
	n := util.U32(buf, 0x04030201)
	require.Equal(t, 4, n)
	require.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, buf)
}

func TestU64(t *testing.T) {
	buf := make([]byte, 8)
	n := util.U64(buf, 0x0807060504030201)
	require.Equal(t, 8, n)
	require.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, buf)
}

func TestWritePrefixedString(t *testing.T) {
	buf := make([]byte, 4+4)
	n := util.WritePrefixedString(buf, "test")
	require.Equal(t, 8, n)
	require.Equal(t, []byte{0x04, 0x00, 0x00, 0x00, 't', 'e', 's', 't'}, buf)
}

func TestReadPrefixedString(t *testing.T) {
	var s string
	n := util.ReadPrefixedString([]byte{0x04, 0x00, 0x00, 0x00, 't', 'e', 's', 't'}, &s)
	require.Equal(t, 8, n)
	require.Equal(t, "test", s)
}

func TestDecodeU32(t *testing.T) {
	t.Run("successful decode", func(t *testing.T) {
		x := uint32(500e6)
		dst := make([]byte, 4)
		_ = util.U32(dst, x)
		n, err := util.DecodeU32(bytes.NewReader(dst))
		require.NoError(t, err)
		require.Equal(t, x, n)
	})
	t.Run("short buffer", func(t *testing.T) {
		dst := make([]byte, 3)
		_, err := util.DecodeU32(bytes.NewReader(dst))
		require.Error(t, err)
	})
}

func TestDecodePrefixedString(t *testing.T) {
	t.Run("successful decode", func(t *testing.T) {
		s := "hello"
		dst := make([]byte, 4+len(s))
		_ = util.WritePrefixedString(dst, s)
		output, err := util.DecodePrefixedString(bytes.NewReader(dst))
		require.NoError(t, err)
		require.Equal(t, s, output)
	})
	t.Run("short buffer", func(t *testing.T) {
		dst := make([]byte, 3)
		_, err := util.DecodePrefixedString(bytes.NewReader(dst))
		require.Error(t, err)
	})
	t.Run("incorrect length", func(t *testing.T) {
		s := "world"
		dst := make([]byte, 4+len(s))
		_ = util.WritePrefixedString(dst, s)
		_, err := util.DecodePrefixedString(bytes.NewReader(dst[:len(dst)-1]))
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})
}
