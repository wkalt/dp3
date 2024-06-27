package ros1msg_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/util/ros1msg"
	"github.com/wkalt/dp3/server/util/testutils"
)

func TestSkipBool(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1})
	err := parser.SkipBool()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipBool()
	require.Error(t, err)
}

func TestSkipInt8(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1})
	err := parser.SkipInt8()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipInt8()
	require.Error(t, err)
}

func TestSkipInt16(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1, 0})
	err := parser.SkipInt16()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipInt16()
	require.Error(t, err)
}

func TestSkipInt32(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1, 0, 0, 0})
	err := parser.SkipInt32()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipInt32()
	require.Error(t, err)
}

func TestSkipInt64(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1, 0, 0, 0, 0, 0, 0, 0})
	err := parser.SkipInt64()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipInt64()
	require.Error(t, err)
}

func TestSkipUint8(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1})
	err := parser.SkipUint8()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipUint8()
	require.Error(t, err)
}

func TestSkipUint16(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1, 0})
	err := parser.SkipUint16()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipUint16()
	require.Error(t, err)
}

func TestSkipUint32(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1, 0, 0, 0})
	err := parser.SkipUint32()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipUint32()
	require.Error(t, err)
}

func TestSkipUint64(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1, 0, 0, 0, 0, 0, 0, 0})
	err := parser.SkipUint64()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipUint64()
	require.Error(t, err)
}

func TestSkipFloat32(t *testing.T) {
	parser := ros1msg.NewDecoder(testutils.U32b(42))
	err := parser.SkipFloat32()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipFloat32()
	require.Error(t, err)
}

func TestSkipFloat64(t *testing.T) {
	parser := ros1msg.NewDecoder(testutils.U64b(42))
	err := parser.SkipFloat64()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipFloat64()
	require.Error(t, err)
}

func TestSkipString(t *testing.T) {
	parser := ros1msg.NewDecoder(testutils.PrefixedString("hello"))
	err := parser.SkipString()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipString()
	require.Error(t, err)
}

func TestSkipChar(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1})
	err := parser.SkipChar()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipChar()
	require.Error(t, err)
}

func TestSkipTime(t *testing.T) {
	parser := ros1msg.NewDecoder(testutils.U64b(42))
	err := parser.SkipTime()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipTime()
	require.Error(t, err)
}

func TestSkipDuration(t *testing.T) {
	parser := ros1msg.NewDecoder(testutils.U64b(42))
	err := parser.SkipDuration()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipDuration()
	require.Error(t, err)
}

func TestSkipByte(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1})
	err := parser.SkipByte()
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipByte()
	require.Error(t, err)
}

func TestSkipBytes(t *testing.T) {
	parser := ros1msg.NewDecoder(testutils.Flatten(
		[]byte{1, 2},
	))
	err := parser.SkipBytes(2)
	require.NoError(t, err)

	parser.Set([]byte{})
	err = parser.SkipBytes(2)
	require.Error(t, err)
}

func TestParseBytes(t *testing.T) {
	parser := ros1msg.NewDecoder(testutils.Flatten(
		[]byte{1, 2},
	))
	bytes, err := parser.Bytes(2)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2}, bytes)
}

func TestArrayLength(t *testing.T) {
	parser := ros1msg.NewDecoder(testutils.U32b(42))
	n, err := parser.ArrayLength()
	require.NoError(t, err)
	require.Equal(t, 42, int(n))

	parser = ros1msg.NewDecoder([]byte{1})
	_, err = parser.ArrayLength()
	require.Error(t, err)
}

func TestParseBool(t *testing.T) {
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
}

func TestParseChar(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1})
	c, err := parser.Char()
	require.NoError(t, err)
	require.Equal(t, byte(1), c)

	parser.Set([]byte{})
	_, err = parser.Char()
	require.Error(t, err)
}

func TestParseByte(t *testing.T) {
	parser := ros1msg.NewDecoder([]byte{1})
	b, err := parser.Byte()
	require.NoError(t, err)
	require.Equal(t, byte(1), b)

	parser.Set([]byte{})
	_, err = parser.Byte()
	require.Error(t, err)
}

func TestParseInt8(t *testing.T) {
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
}

func TestParseInt16(t *testing.T) {
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
}

func TestParseInt32(t *testing.T) {
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
}

func TestParseInt64(t *testing.T) {
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
}

func TestParseUint8(t *testing.T) {
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
}

func TestParseUint16(t *testing.T) {
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
}

func TestParseUint32(t *testing.T) {
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
}

func TestParseUint64(t *testing.T) {
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
}

func TestParseFloat32(t *testing.T) {
	f := float32(3.14)
	parser := ros1msg.NewDecoder(testutils.U32b(math.Float32bits(f)))
	f1, err := parser.Float32()
	require.NoError(t, err)
	require.InEpsilon(t, f, f1, 0.001)
	parser.Set([]byte{})
	_, err = parser.Float32()
	require.Error(t, err)
}

func TestParseFloat64(t *testing.T) {
	f := float64(3.14)
	parser := ros1msg.NewDecoder(testutils.U64b(math.Float64bits(f)))
	f1, err := parser.Float64()
	require.NoError(t, err)
	require.InEpsilon(t, f, f1, 0.001)
	parser.Set([]byte{})
	_, err = parser.Float64()
	require.Error(t, err)
}

func TestParseString(t *testing.T) {
	parser := ros1msg.NewDecoder(testutils.PrefixedString("hello"))
	s, err := parser.String()
	require.NoError(t, err)
	require.Equal(t, "hello", s)

	parser.Set([]byte{})
	_, err = parser.String()
	require.Error(t, err)
}

func TestParseTime(t *testing.T) {
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
}

func TestParseDuration(t *testing.T) {
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
}

func TestMessageParser(t *testing.T) {
}
