package ros1msg_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/ros1msg"
	"github.com/wkalt/dp3/util/schema"
)

// newSchema creates a new schema.
func newSchema(fields ...schema.Field) schema.Schema {
	return schema.Schema{
		Name:   "",
		Fields: fields,
	}
}

// newPrimitiveType creates a new primitive type.
func newPrimitiveType(p schema.PrimitiveType) schema.Type {
	return schema.Type{
		Primitive: p,
	}
}

// newArrayType creates a new array type.
func newArrayType(size int, items schema.Type) schema.Type {
	return schema.Type{
		Array:     true,
		FixedSize: size,
		Items:     &items,
	}
}

// newRecordType creates a new record type.
func newRecordType(fields []schema.Field) schema.Type {
	return schema.Type{
		Record: true,
		Fields: fields,
	}
}

// newSchemaField creates a new field.
func newSchemaField(name string, typ schema.Type) schema.Field {
	return schema.Field{
		Name: name,
		Type: typ,
	}
}

func TestSchemaAnalyzer(t *testing.T) {
	cases := []struct {
		assertion string
		schema    schema.Schema
		expected  []util.Named[schema.PrimitiveType]
	}{
		{
			"primitives",
			newSchema(
				newSchemaField("field1", newPrimitiveType(schema.INT8)),
				newSchemaField("field2", newPrimitiveType(schema.INT16)),
			),
			[]util.Named[schema.PrimitiveType]{
				util.NewNamed("field1", schema.INT8),
				util.NewNamed("field2", schema.INT16),
			},
		},
		{
			"complex type",
			newSchema(
				newSchemaField("field1", newRecordType([]schema.Field{
					newSchemaField("subfield1", newPrimitiveType(schema.INT8)),
				})),
			),
			[]util.Named[schema.PrimitiveType]{
				util.NewNamed("field1.subfield1", schema.INT8),
			},
		},
		{
			"short fixed length arrays",
			newSchema(
				newSchemaField("field1", newPrimitiveType(schema.INT8)),
				newSchemaField("field2", newArrayType(3, newPrimitiveType(schema.INT16))),
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
			newSchema(
				newSchemaField("field1", newPrimitiveType(schema.INT8)),
				newSchemaField("field2", newArrayType(0, newPrimitiveType(schema.INT8))),
			),
			[]util.Named[schema.PrimitiveType]{
				util.NewNamed("field1", schema.INT8),
			},
		},
		{
			"complex fixed-length array",
			newSchema(
				newSchemaField("field1", newPrimitiveType(schema.INT8)),
				newSchemaField("field2", newArrayType(2, newRecordType([]schema.Field{
					newSchemaField("subfield1", newPrimitiveType(schema.INT16)),
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
