package ros1msg_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/ros1msg"
	"github.com/wkalt/dp3/util/schema"
	"github.com/wkalt/dp3/util/testutils"
)

func TestSchemaAnalyzer(t *testing.T) {
	cases := []struct {
		assertion string
		schema    schema.Schema
		expected  []util.Named[schema.PrimitiveType]
	}{
		{
			"primitives",
			schema.NewSchema("",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewPrimitiveType(schema.INT16)),
			),
			[]util.Named[schema.PrimitiveType]{
				util.NewNamed("field1", schema.INT8),
				util.NewNamed("field2", schema.INT16),
			},
		},
		{
			"complex type",
			schema.NewSchema("",
				schema.NewField("field1", schema.NewRecordType([]schema.Field{
					schema.NewField("subfield1", schema.NewPrimitiveType(schema.INT8)),
				})),
			),
			[]util.Named[schema.PrimitiveType]{
				util.NewNamed("field1.subfield1", schema.INT8),
			},
		},
		{
			"short fixed length arrays",
			schema.NewSchema("",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewArrayType(3, schema.NewPrimitiveType(schema.INT16))),
			),
			[]util.Named[schema.PrimitiveType]{
				util.NewNamed("field1", schema.INT8),
				util.NewNamed("field2[0]", schema.INT16),
				util.NewNamed("field2[1]", schema.INT16),
				util.NewNamed("field2[2]", schema.INT16),
			},
		},
		{
			"variable length arrays are skipped",
			schema.NewSchema("",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewArrayType(0, schema.NewPrimitiveType(schema.INT8))),
			),
			[]util.Named[schema.PrimitiveType]{
				util.NewNamed("field1", schema.INT8),
			},
		},
		{
			"complex fixed-length array",
			schema.NewSchema("",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewArrayType(2, schema.NewRecordType([]schema.Field{
					schema.NewField("subfield1", schema.NewPrimitiveType(schema.INT16)),
				}))),
			),
			[]util.Named[schema.PrimitiveType]{
				util.NewNamed("field1", schema.INT8),
				util.NewNamed("field2[0].subfield1", schema.INT16),
				util.NewNamed("field2[1].subfield1", schema.INT16),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			types := ros1msg.AnalyzeSchema(c.schema)
			require.Equal(t, c.expected, types)
		})
	}
}

func TestParseMessage(t *testing.T) {
	cases := []struct {
		assertion string
		schema    schema.Schema
		data      []byte
		expected  []any
	}{
		{
			"primitives",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewPrimitiveType(schema.INT16)),
			),
			testutils.Flatten(testutils.U8b(1), testutils.U16b(2)),
			[]any{int8(1), int16(2)},
		},
		{
			"records",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewRecordType([]schema.Field{
					schema.NewField("subfield1", schema.NewPrimitiveType(schema.INT8)),
				})),
			),
			testutils.Flatten(testutils.U8b(1)),
			[]any{int8(1)},
		},
		{
			"nested record",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewRecordType([]schema.Field{
					schema.NewField("subfield1", schema.NewRecordType([]schema.Field{
						schema.NewField("subsubfield1", schema.NewPrimitiveType(schema.INT8)),
					})),
				})),
			),
			testutils.Flatten(testutils.U8b(1)),
			[]any{int8(1)},
		},
		{
			"variable length arrays are skipped",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewArrayType(0, schema.NewPrimitiveType(schema.INT8))),
			),
			testutils.Flatten(testutils.U8b(1), testutils.U32b(1), testutils.U8b(2)),
			[]any{int8(1)},
		},
		{
			"fixed-length arrays under 10 are emitted",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewArrayType(2, schema.NewPrimitiveType(schema.INT8))),
			),
			testutils.Flatten(testutils.U8b(1), testutils.U8b(3), testutils.U8b(2)),
			[]any{int8(1), int8(3), int8(2)},
		},
		{
			"fixed-length arrays over 10 are skipped",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewArrayType(11, schema.NewPrimitiveType(schema.INT8))),
			),
			testutils.Flatten(testutils.U8b(1),
				testutils.U8b(2), testutils.U8b(2), testutils.U8b(2), testutils.U8b(2), // eleven bytes
				testutils.U8b(2), testutils.U8b(2), testutils.U8b(2), testutils.U8b(2),
				testutils.U8b(2), testutils.U8b(2), testutils.U8b(2)),
			[]any{int8(1)},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			fields := ros1msg.AnalyzeSchema(c.schema)
			values := make([]any, 0, len(fields))
			require.NoError(t, ros1msg.ParseMessage(ros1msg.GenParser(c.schema), c.data, &values))
		})
	}
}

func TestPrimitiveFieldParsing(t *testing.T) {
	cases := []struct {
		assertion string
		schema    schema.Schema
		data      []byte
		expected  any
	}{
		{
			"bool",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.BOOL))),
			[]byte{0x01},
			true,
		},
		{
			"int8",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.INT8))),
			[]byte{0x01},
			int8(1),
		},
		{
			"int16",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.INT16))),
			testutils.U16b(1),
			int16(1),
		},
		{
			"int32",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.INT32))),
			testutils.U32b(1),
			int32(1),
		},
		{
			"int64",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.INT64))),
			testutils.U64b(1),
			int64(1),
		},
		{
			"uint8",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.UINT8))),
			[]byte{0x01},
			uint8(1),
		},
		{
			"uint16",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.UINT16))),
			testutils.U16b(1),
			uint16(1),
		},
		{
			"uint32",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.UINT32))),
			testutils.U32b(1),
			uint32(1),
		},
		{
			"uint64",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.UINT64))),
			testutils.U64b(1),
			uint64(1),
		},
		{
			"float32",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.FLOAT32))),
			testutils.F32b(1),
			float32(1),
		},
		{
			"float64",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.FLOAT64))),
			testutils.F64b(1),
			float64(1),
		},
		{
			"string",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.STRING))),
			testutils.Flatten(testutils.U32b(1), []byte("a")),
			"a",
		},
		{
			"time",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.TIME))),
			testutils.Flatten(testutils.U32b(1), testutils.U32b(1)),
			uint64(1e9 + 1),
		},
		{
			"duration",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.DURATION))),
			testutils.Flatten(testutils.U32b(1), testutils.U32b(1)),
			int64(1e9 + 1),
		},
		{
			"char",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.CHAR))),
			[]byte{0x01},
			rune(0x01),
		},
		{
			"byte",
			schema.NewSchema("", schema.NewField("field1", schema.NewPrimitiveType(schema.BYTE))),
			[]byte{0x01},
			byte(0x01),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			fields := ros1msg.AnalyzeSchema(c.schema)
			values := make([]any, 0, len(fields))
			require.NoError(t, ros1msg.ParseMessage(ros1msg.GenParser(c.schema), c.data, &values))
			require.Len(t, values, 1)
			require.Equal(t, c.expected, values[0])

			t.Run("handles short reads", func(t *testing.T) {
				require.ErrorIs(t,
					ros1msg.ParseMessage(ros1msg.GenParser(c.schema), []byte{}, &values),
					ros1msg.ShortReadError{},
				)
			})
		})
	}
}

func TestFieldParsing(t *testing.T) {
	cases := []struct {
		assertion string
		schema    schema.Schema
		data      []byte
		expected  map[util.Named[schema.PrimitiveType]]any
	}{
		{
			"primitives",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewPrimitiveType(schema.INT16)),
			),
			testutils.Flatten(testutils.U8b(1), testutils.U16b(2)),
			map[util.Named[schema.PrimitiveType]]any{
				util.NewNamed("field1", schema.INT8):  int8(1),
				util.NewNamed("field2", schema.INT16): int16(2),
			},
		},
		{
			"records",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewRecordType([]schema.Field{
					schema.NewField("subfield1", schema.NewPrimitiveType(schema.INT8)),
				})),
			),
			testutils.Flatten(testutils.U8b(1)),
			map[util.Named[schema.PrimitiveType]]any{
				util.NewNamed("field1.subfield1", schema.INT8): int8(1),
			},
		},
		{
			"nested record",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewRecordType([]schema.Field{
					schema.NewField("subfield1", schema.NewRecordType([]schema.Field{
						schema.NewField("subsubfield1", schema.NewPrimitiveType(schema.INT8)),
					})),
				})),
			),
			testutils.Flatten(testutils.U8b(1)),
			map[util.Named[schema.PrimitiveType]]any{
				util.NewNamed("field1.subfield1.subsubfield1", schema.INT8): int8(1),
			},
		},
		{
			"variable length arrays are skipped",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewArrayType(0, schema.NewPrimitiveType(schema.INT8))),
			),
			testutils.Flatten(testutils.U8b(1), testutils.U32b(1), testutils.U8b(2)),
			map[util.Named[schema.PrimitiveType]]any{
				util.NewNamed("field1", schema.INT8): int8(1),
			},
		},
		{
			"fixed-length arrays under 10 are emitted",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewArrayType(2, schema.NewPrimitiveType(schema.INT8))),
			),
			testutils.Flatten(testutils.U8b(1), testutils.U8b(3), testutils.U8b(2)),
			map[util.Named[schema.PrimitiveType]]any{
				util.NewNamed("field1", schema.INT8):    int8(1),
				util.NewNamed("field2[0]", schema.INT8): int8(3),
				util.NewNamed("field2[1]", schema.INT8): int8(2),
			},
		},
		{
			"fixed-length arrays over 10 are skipped",
			schema.NewSchema(
				"",
				schema.NewField("field1", schema.NewPrimitiveType(schema.INT8)),
				schema.NewField("field2", schema.NewArrayType(11, schema.NewPrimitiveType(schema.INT8))),
			),
			testutils.Flatten(testutils.U8b(1),
				testutils.U8b(1), testutils.U8b(1), testutils.U8b(1), // eleven bytes
				testutils.U8b(1), testutils.U8b(1), testutils.U8b(1),
				testutils.U8b(1), testutils.U8b(1), testutils.U8b(1),
				testutils.U8b(1), testutils.U8b(1)),
			map[util.Named[schema.PrimitiveType]]any{
				util.NewNamed("field1", schema.INT8): int8(1),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			fields := ros1msg.AnalyzeSchema(c.schema)
			values := make([]any, 0, len(fields))
			require.NoError(t, ros1msg.ParseMessage(ros1msg.GenParser(c.schema), c.data, &values))
			m := make(map[util.Named[schema.PrimitiveType]]any)
			for i, f := range fields {
				m[f] = values[i]
			}
			require.Equal(t, c.expected, m)
		})
	}
}
