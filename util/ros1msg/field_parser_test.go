package ros1msg_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/ros1msg"
	"github.com/wkalt/dp3/util/schema"
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
