package testutils_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/testutils"
)

func TestGetOpenPort(t *testing.T) {
	_, err := testutils.GetOpenPort()
	require.NoError(t, err)
}

func TestFlatten(t *testing.T) {
	cases := []struct {
		assertion string
		in        []int
		expected  []int
	}{
		{
			"empty",
			[]int{},
			[]int{},
		},
		{
			"single",
			[]int{1},
			[]int{1},
		},
		{
			"multiple",
			[]int{1, 2, 3},
			[]int{1, 2, 3},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			require.Equal(t, c.expected, testutils.Flatten(c.in))
		})
	}
}

func TestU8b(t *testing.T) {
	cases := []struct {
		assertion string
		in        uint8
		expected  []byte
	}{
		{
			"zero",
			0,
			[]byte{0},
		},
		{
			"one",
			1,
			[]byte{1},
		},
		{
			"max",
			255,
			[]byte{255},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			require.Equal(t, c.expected, testutils.U8b(c.in))
		})
	}
}

func TestU16b(t *testing.T) {
	cases := []struct {
		assertion string
		in        uint16
		expected  []byte
	}{
		{
			"zero",
			0,
			[]byte{0, 0},
		},
		{
			"one",
			1,
			[]byte{1, 0},
		},
		{
			"max",
			65535,
			[]byte{255, 255},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			require.Equal(t, c.expected, testutils.U16b(c.in))
		})
	}
}

func TestU32b(t *testing.T) {
	cases := []struct {
		assertion string
		in        uint32
		expected  []byte
	}{
		{
			"zero",
			0,
			[]byte{0, 0, 0, 0},
		},
		{
			"one",
			1,
			[]byte{1, 0, 0, 0},
		},
		{
			"max",
			4294967295,
			[]byte{255, 255, 255, 255},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			require.Equal(t, c.expected, testutils.U32b(c.in))
		})
	}
}

func TestU64b(t *testing.T) {
	cases := []struct {
		assertion string
		in        uint64
		expected  []byte
	}{
		{
			"zero",
			0,
			[]byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			"one",
			1,
			[]byte{1, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			"max",
			18446744073709551615,
			[]byte{255, 255, 255, 255, 255, 255, 255, 255},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			require.Equal(t, c.expected, testutils.U64b(c.in))
		})
	}
}

func TestF32b(t *testing.T) {
	cases := []struct {
		assertion string
		in        float32
		expected  []byte
	}{
		{
			"zero",
			0,
			[]byte{0, 0, 0, 0},
		},
		{
			"one",
			1,
			[]byte{0, 0, 128, 63},
		},
		{
			"max",
			math.MaxFloat32,
			[]byte{0xff, 0xff, 0x7f, 0x7f},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			require.Equal(t, c.expected, testutils.F32b(c.in))
		})
	}
}

func TestF64b(t *testing.T) {
	cases := []struct {
		assertion string
		in        float64
		expected  []byte
	}{
		{
			"zero",
			0,
			[]byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			"one",
			1,
			[]byte{0, 0, 0, 0, 0, 0, 240, 63},
		},
		{
			"max",
			math.MaxFloat64,
			[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0x7f},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			require.Equal(t, c.expected, testutils.F64b(c.in))
		})
	}
}

func TestPrefixedString(t *testing.T) {
	cases := []struct {
		assertion string
		in        string
		expected  []byte
	}{
		{
			"empty",
			"",
			[]byte{0, 0, 0, 0},
		},
		{
			"one",
			"1",
			[]byte{1, 0, 0, 0, 49},
		},
		{
			"max",
			"max",
			[]byte{3, 0, 0, 0, 109, 97, 120},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			require.Equal(t, c.expected, testutils.PrefixedString(c.in))
		})
	}
}

func TestReadPrefixedString(t *testing.T) {
	cases := []struct {
		assertion string
		in        []byte
		expected  string
	}{
		{
			"empty",
			[]byte{0, 0, 0, 0},
			"",
		},
		{
			"one",
			[]byte{1, 0, 0, 0, 49},
			"1",
		},
		{
			"max",
			[]byte{3, 0, 0, 0, 109, 97, 120},
			"max",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			require.Equal(t, c.expected, testutils.ReadPrefixedString(t, c.in))
		})
	}
}
