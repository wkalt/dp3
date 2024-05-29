package schema_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/ros1msg"
	"github.com/wkalt/dp3/util/schema"
	"github.com/wkalt/dp3/util/testutils"
)

func TestROS1MessageParser(t *testing.T) { // nolint: maintidx
	cases := []struct {
		assertion      string
		msgdef         string
		input          []byte
		selections     []string
		expectedOutput []any
		expectedBreaks []int
	}{
		{
			"complex fixed-length array nested element",
			`Foo[2] a
			===
			MSG: pkg/Foo
			string s`,
			testutils.Flatten(
				testutils.PrefixedString("hello"),
				testutils.PrefixedString("world"),
			),
			[]string{"a[1].s"},
			[]any{"world"},
			[]int{2},
		},
		{
			"complex fixed-length array with complex fixed nested element",
			`Foo[1] a
			===
			MSG: pkg/Foo
			Bar[2] b
			===
			MSG: pkg/Bar
			string s`,
			testutils.Flatten(
				testutils.PrefixedString("hello"),
				testutils.PrefixedString("world"),
			),
			[]string{"a[0].b[1].s"},
			[]any{"world"},
			[]int{1, 2},
		},
		{
			"byte array",
			"uint8[] bytes",
			testutils.Flatten(
				testutils.U32b(3),
				testutils.U8b(1),
				testutils.U8b(2),
				testutils.U8b(3),
			),
			nil,
			[]any{[]byte{1, 2, 3}},
			[]int{3},
		},
		{
			"project an array element and subsequent field",
			`int32[] a
			 uint32 b
			`,
			testutils.Flatten(
				testutils.U32b(3),
				testutils.U32b(1),
				testutils.U32b(2),
				testutils.U32b(3),
				testutils.U32b(42),
			),
			[]string{"a[1]", "b"},
			[]any{int32(2), uint32(42)},
			[]int{3},
		},
		{
			"project an array element",
			"int32[] a",
			testutils.Flatten(
				testutils.U32b(3),
				testutils.U32b(1),
				testutils.U32b(2),
				testutils.U32b(3),
			),
			[]string{"a[1]"},
			[]any{int32(2)},
			[]int{3},
		},
		{
			"complex type subtype projected",
			`Foo a
			===
			MSG: pkg/Foo
			string s
			int32 i`,
			testutils.Flatten(
				testutils.PrefixedString("hello"),
				testutils.I32b(42),
			),
			[]string{"a.s"},
			[]any{"hello"},
			nil,
		},
		{
			"complex type no arrays",
			`Foo a
			===
			MSG: pkg/Foo
			string s
			int32 i`,
			testutils.Flatten(
				testutils.PrefixedString("hello"),
				testutils.I32b(42),
			),
			nil,
			[]any{"hello", int32(42)},
			nil,
		},
		{
			"complex fixed-length array with variable-length array subelements",
			`Foo[2] a
			===
			MSG: pkg/Foo
			int32[] i`,
			testutils.Flatten(
				testutils.U32b(2),
				testutils.I32b(1),
				testutils.I32b(2),
				testutils.U32b(2),
				testutils.I32b(3),
				testutils.I32b(4),
			),
			nil,
			[]any{int32(1), int32(2), int32(3), int32(4)},
			[]int{2, 2, 2},
		},
		{
			"complex fixed-length array with nested fixed-length array subelements",
			`Foo[2] a
			===
			MSG: pkg/Foo
			int32[2] i`,
			testutils.Flatten(
				testutils.I32b(1),
				testutils.I32b(2),
				testutils.I32b(3),
				testutils.I32b(4),
			),
			nil,
			[]any{int32(1), int32(2), int32(3), int32(4)},
			[]int{2, 2, 2},
		},
		{
			"complex variable-length array with nested fixed-length array subelements",
			`Foo[] a
			===
			MSG: pkg/Foo
			int32[2] i`,
			testutils.Flatten(
				testutils.U32b(2),

				testutils.I32b(1),
				testutils.I32b(2),

				testutils.I32b(3),
				testutils.I32b(4),
			),
			nil,
			[]any{int32(1), int32(2), int32(3), int32(4)},
			[]int{2, 2, 2},
		},
		{
			"complex variable-length array with nested array subelements",
			`Foo[] a
			===
			MSG: pkg/Foo
			int32[] i
			string[] s`,
			testutils.Flatten(
				testutils.U32b(2),

				testutils.U32b(2),
				testutils.I32b(1),
				testutils.I32b(2),

				testutils.U32b(2),
				testutils.PrefixedString("hello"),
				testutils.PrefixedString("world"),

				testutils.U32b(2),
				testutils.I32b(3),
				testutils.I32b(4),

				testutils.U32b(2),
				testutils.PrefixedString("foo"),
				testutils.PrefixedString("bar"),
			),
			nil,
			[]any{int32(1), int32(2), "hello", "world", int32(3), int32(4), "foo", "bar"},
			[]int{2, 2, 2, 2, 2},
		},
		{
			"complex variable-length array",
			`Foo[] a
		          ===
		          MSG: pkg/Foo
		          string s
		          int32 i`,
			testutils.Flatten(
				testutils.U32b(2),
				testutils.PrefixedString("hello"),
				testutils.I32b(42),
				testutils.PrefixedString("world"),
				testutils.I32b(43),
			),
			nil,
			[]any{"hello", int32(42), "world", int32(43)},
			[]int{2},
		},
		{
			"complex fixed-length array",
			`Foo[2] a
		    ===
		    MSG: pkg/Foo
		    string s
		    int32 i`,
			testutils.Flatten(
				testutils.PrefixedString("hello"),
				testutils.I32b(42),
				testutils.PrefixedString("world"),
				testutils.I32b(43),
			),
			nil,
			[]any{"hello", int32(42), "world", int32(43)},
			[]int{2},
		},
		{
			"bool",
			"bool b",
			[]byte{1},
			[]string{"b"},
			[]any{true},
			nil,
		},
		{
			"int8",
			"int8 i",
			[]byte{1},
			[]string{"i"},
			[]any{int8(1)},
			nil,
		},
		{
			"int16",
			"int16 i",
			[]byte{0, 1},
			[]string{"i"},
			[]any{int16(256)},
			nil,
		},
		{
			"int32",
			"int32 i",
			[]byte{0, 0, 1, 0},
			[]string{"i"},
			[]any{int32(65536)},
			nil,
		},
		{
			"int64",
			"int64 i",
			[]byte{0, 0, 0, 0, 0, 1, 0, 0},
			[]string{"i"},
			[]any{int64(1099511627776)},
			nil,
		},
		{
			"uint8",
			"uint8 i",
			[]byte{1},
			[]string{"i"},
			[]any{uint8(1)},
			nil,
		},
		{
			"uint16",
			"uint16 i",
			[]byte{0, 1},
			[]string{"i"},
			[]any{uint16(256)},
			nil,
		},
		{
			"uint32",
			"uint32 i",
			[]byte{0, 0, 1, 0},
			[]string{"i"},
			[]any{uint32(65536)},
			nil,
		},
		{
			"uint64",
			"uint64 i",
			[]byte{0, 0, 0, 0, 0, 1, 0, 0},
			[]string{"i"},
			[]any{uint64(1099511627776)},
			nil,
		},
		{
			"float32",
			"float32 f",
			testutils.F32b(float32(math.Pi)),
			[]string{"f"},
			[]any{float32(math.Pi)},
			nil,
		},
		{
			"float64",
			"float64 f",
			testutils.F64b(float64(math.Pi)),
			[]string{"f"},
			[]any{float64(math.Pi)},
			nil,
		},
		{
			"time",
			"time t",
			[]byte{1, 0, 0, 0, 1, 0, 0, 0},
			[]string{"t"},
			[]any{uint64(1e9 + 1)},
			nil,
		},
		{
			"duration",
			"duration d",
			[]byte{1, 0, 0, 0, 1, 0, 0, 0},
			[]string{"d"},
			[]any{uint64(1e9 + 1)},
			nil,
		},
		{
			"string",
			"string s",
			testutils.PrefixedString("hello"),
			[]string{"s"},
			[]any{"hello"},
			nil,
		},
		{
			"two fields first projected",
			`string foo
			string bar`,
			testutils.Flatten(
				testutils.PrefixedString("hello"),
				testutils.PrefixedString("world"),
			),
			[]string{"foo"},
			[]any{"hello"},
			nil,
		},
		{
			"two fields second projected",
			`string foo
			string bar`,
			testutils.Flatten(
				testutils.PrefixedString("hello"),
				testutils.PrefixedString("world"),
			),
			[]string{"bar"},
			[]any{"world"},
			nil,
		},
		{
			"two fields both projected",
			`string foo
			string bar`,
			testutils.Flatten(
				testutils.PrefixedString("hello"),
				testutils.PrefixedString("world"),
			),
			nil,
			[]any{"hello", "world"},
			nil,
		},
		{
			"primitive variable-length array",
			"int32[] a",
			testutils.Flatten(
				testutils.U32b(3),
				testutils.U32b(1),
				testutils.U32b(2),
				testutils.U32b(3),
			),
			nil,
			[]any{int32(1), int32(2), int32(3)},
			[]int{3},
		},
		{
			"primitive fixed-length array",
			"int32[3] a",
			testutils.Flatten(
				testutils.U32b(1),
				testutils.U32b(2),
				testutils.U32b(3),
			),
			nil,
			[]any{int32(1), int32(2), int32(3)},
			[]int{3},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			parsed, err := ros1msg.ParseROS1MessageDefinition("pkg", "type", []byte(c.msgdef))
			require.NoError(t, err)

			decoder := ros1msg.NewDecoder(nil)
			parser, err := schema.NewParser(parsed, c.selections, decoder)
			require.NoError(t, err)

			breaks, values, err := parser.Parse(c.input)
			require.NoError(t, err)
			require.Equal(t, c.expectedOutput, values)
			require.Equal(t, c.expectedBreaks, breaks)
		})
	}
}
