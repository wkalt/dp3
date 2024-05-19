package ros1msg_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/ros1msg"
	"github.com/wkalt/dp3/util/testutils"
)

/*

 */

func TestMessageParser(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		parser := ros1msg.NewDecoder([]byte{1})
		b, err := parser.Bool()
		require.NoError(t, err)
		require.True(t, b)

		parser.Set([]byte{0})
		b, err = parser.Bool()
		require.NoError(t, err)
		require.False(t, b)

		parser.Set([]byte{})
		_, err = parser.Bool()
		require.Error(t, err)
	})

	t.Run("int8", func(t *testing.T) {
		parser := ros1msg.NewDecoder([]byte{1})
		i, err := parser.Int8()
		require.NoError(t, err)
		require.Equal(t, int8(1), i)

		parser.Set([]byte{0})
		i, err = parser.Int8()
		require.NoError(t, err)
		require.Equal(t, int8(0), i)

		parser.Set([]byte{})
		_, err = parser.Int8()
		require.Error(t, err)
	})

	t.Run("int16", func(t *testing.T) {
		parser := ros1msg.NewDecoder([]byte{1, 0})
		i, err := parser.Int16()
		require.NoError(t, err)
		require.Equal(t, int16(1), i)

		parser.Set([]byte{0, 1})
		i, err = parser.Int16()
		require.NoError(t, err)
		require.Equal(t, int16(256), i)

		parser.Set([]byte{})
		_, err = parser.Int16()
		require.Error(t, err)
	})

	t.Run("int32", func(t *testing.T) {
		parser := ros1msg.NewDecoder([]byte{1, 0, 0, 0})
		i, err := parser.Int32()
		require.NoError(t, err)
		require.Equal(t, int32(1), i)

		parser.Set([]byte{0, 1, 0, 0})
		i, err = parser.Int32()
		require.NoError(t, err)
		require.Equal(t, int32(256), i)

		parser.Set([]byte{})
		_, err = parser.Int32()
		require.Error(t, err)
	})

	t.Run("int64", func(t *testing.T) {
		parser := ros1msg.NewDecoder([]byte{1, 0, 0, 0, 0, 0, 0, 0})
		i, err := parser.Int64()
		require.NoError(t, err)
		require.Equal(t, int64(1), i)

		parser.Set([]byte{0, 1, 0, 0, 0, 0, 0, 0})
		i, err = parser.Int64()
		require.NoError(t, err)
		require.Equal(t, int64(256), i)

		parser.Set([]byte{})
		_, err = parser.Int64()
		require.Error(t, err)
	})

	t.Run("uint8", func(t *testing.T) {
		parser := ros1msg.NewDecoder([]byte{1})
		i, err := parser.Uint8()
		require.NoError(t, err)
		require.Equal(t, uint8(1), i)

		parser.Set([]byte{0})
		i, err = parser.Uint8()
		require.NoError(t, err)
		require.Equal(t, uint8(0), i)

		parser.Set([]byte{})
		_, err = parser.Uint8()
		require.Error(t, err)
	})

	t.Run("uint16", func(t *testing.T) {
		parser := ros1msg.NewDecoder([]byte{1, 0})
		i, err := parser.Uint16()
		require.NoError(t, err)
		require.Equal(t, uint16(1), i)

		parser.Set([]byte{0, 1})
		i, err = parser.Uint16()
		require.NoError(t, err)
		require.Equal(t, uint16(256), i)

		parser.Set([]byte{})
		_, err = parser.Uint16()
		require.Error(t, err)
	})

	t.Run("uint32", func(t *testing.T) {
		parser := ros1msg.NewDecoder([]byte{1, 0, 0, 0})
		i, err := parser.Uint32()
		require.NoError(t, err)
		require.Equal(t, uint32(1), i)

		parser.Set([]byte{0, 1, 0, 0})
		i, err = parser.Uint32()
		require.NoError(t, err)
		require.Equal(t, uint32(256), i)

		parser.Set([]byte{})
		_, err = parser.Uint32()
		require.Error(t, err)
	})

	t.Run("uint64", func(t *testing.T) {
		parser := ros1msg.NewDecoder([]byte{1, 0, 0, 0, 0, 0, 0, 0})
		i, err := parser.Uint64()
		require.NoError(t, err)
		require.Equal(t, uint64(1), i)

		parser.Set([]byte{0, 1, 0, 0, 0, 0, 0, 0})
		i, err = parser.Uint64()
		require.NoError(t, err)
		require.Equal(t, uint64(256), i)

		parser.Set([]byte{})
		_, err = parser.Uint64()
		require.Error(t, err)
	})

	t.Run("float32", func(t *testing.T) {
		f := float32(3.14)
		parser := ros1msg.NewDecoder(testutils.U32b(math.Float32bits(f)))
		f1, err := parser.Float32()
		require.NoError(t, err)
		require.InEpsilon(t, f, f1, 0.001)
		parser.Set([]byte{})
		_, err = parser.Float32()
		require.Error(t, err)
	})

	t.Run("float64", func(t *testing.T) {
		f := float64(3.14)
		parser := ros1msg.NewDecoder(testutils.U64b(math.Float64bits(f)))
		f1, err := parser.Float64()
		require.NoError(t, err)
		require.InEpsilon(t, f, f1, 0.001)
		parser.Set([]byte{})
		_, err = parser.Float64()
		require.Error(t, err)
	})

	t.Run("string", func(t *testing.T) {
		parser := ros1msg.NewDecoder(testutils.PrefixedString("hello"))
		s, err := parser.String()
		require.NoError(t, err)
		require.Equal(t, "hello", s)

		parser.Set([]byte{})
		_, err = parser.String()
		require.Error(t, err)
	})

	t.Run("time", func(t *testing.T) {
		ts := uint64(time.Now().UnixNano())
		secs := uint32(ts / 1e9)
		nsecs := uint32(ts % 1e9)
		parser := ros1msg.NewDecoder(testutils.Flatten(
			testutils.U32b(secs),
			testutils.U32b(nsecs),
		))
		t1, err := parser.Time()
		require.NoError(t, err)
		require.Equal(t, ts, t1)

		parser.Set([]byte{})
		_, err = parser.Time()
		require.Error(t, err)
	})

	t.Run("duration", func(t *testing.T) {
		ts := uint64(time.Now().UnixNano())
		secs := uint32(ts / 1e9)
		nsecs := uint32(ts % 1e9)
		parser := ros1msg.NewDecoder(testutils.Flatten(
			testutils.U32b(secs),
			testutils.U32b(nsecs),
		))
		d, err := parser.Duration()
		require.NoError(t, err)
		require.Equal(t, ts, d)

		parser.Set([]byte{})
		_, err = parser.Duration()
		require.Error(t, err)
	})
}
