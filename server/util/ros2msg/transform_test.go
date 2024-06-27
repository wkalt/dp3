package ros2msg_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/util/ros2msg"
	"github.com/wkalt/dp3/server/util/schema"
	"github.com/wkalt/dp3/server/util/testutils"
)

func primitiveType(t schema.PrimitiveType) *schema.Type {
	return &schema.Type{
		Primitive: t,
	}
}

func newSchema(fields ...schema.Field) *schema.Schema {
	return &schema.Schema{
		Name:   "test/Test",
		Fields: fields,
	}
}

func newSchemaField(name string, t schema.Type, def ...interface{}) schema.Field {
	f := schema.Field{
		Name: name,
		Type: t,
	}
	if len(def) > 0 {
		f.Default = def[0]
	}
	return f
}

func TestTransform(t *testing.T) {
	cases := []struct {
		assertion string
		msgdef    string
		output    *schema.Schema
	}{
		{
			"primitive",
			"string foo",
			newSchema(newSchemaField("foo", *primitiveType(schema.STRING))),
		},
		{
			"constants skipped",
			`
			string foo
			uint32 CONSTANT=32
			`,
			newSchema(newSchemaField("foo", *primitiveType(schema.STRING))),
		},
		{
			"primitive with default value",
			`string foo "bar"`,
			newSchema(newSchemaField("foo", *primitiveType(schema.STRING), "bar")),
		},
		{
			"primitive with integer default value",
			`int32 foo 42`,
			newSchema(newSchemaField("foo", *primitiveType(schema.INT32), int64(42))),
		},
		{
			"primitive with float default value",
			`float32 foo 3.14`,
			newSchema(newSchemaField("foo", *primitiveType(schema.FLOAT32), float64(3.14))),
		},
		{
			"primitive array",
			"string[10] foo",
			newSchema(newSchemaField("foo", schema.Type{
				Array:     true,
				Items:     primitiveType(schema.STRING),
				FixedSize: 10,
			})),
		},
		{
			"bounded length array",
			"string[<=10] foo",
			newSchema(newSchemaField("foo", schema.Type{
				Array:     true,
				Items:     primitiveType(schema.STRING),
				FixedSize: 10,
				Bounded:   true,
			})),
		},
		{
			"size bounded field with bounded length array",
			"string<=10[<=10] foo",
			newSchema(newSchemaField("foo", schema.Type{
				Array:     true,
				Items:     primitiveType(schema.STRING),
				FixedSize: 10,
				Bounded:   true,
				SizeBound: 10,
			})),
		},
		{
			"bounded length string field",
			"string<=10 foo",
			newSchema(newSchemaField("foo", schema.Type{
				Primitive: schema.STRING,
				SizeBound: 10,
			})),
		},
		{
			"primitive array",
			"string[10] foo",
			newSchema(newSchemaField("foo", schema.Type{
				Array:     true,
				Items:     primitiveType(schema.STRING),
				FixedSize: 10,
			})),
		},
		{
			"subdependency with constant",
			testutils.TrimLeadingSpace(`
			MyType field2
			===
			MSG: test/MyType
			string field3
			uint32 CONSTANT=32
			`),
			newSchema(newSchemaField("field2", schema.Type{
				Record: true,
				Fields: []schema.Field{
					newSchemaField("field3", *primitiveType(schema.STRING)),
				},
			})),
		},
		{
			"subdependencies",
			testutils.TrimLeadingSpace(`
			Header header #for timestamp
			===
			MSG: std_msgs/Header
			uint32 seq
			time stamp
			string frame_id
			`),
			newSchema(newSchemaField("header", schema.Type{
				Record: true,
				Fields: []schema.Field{
					newSchemaField("seq", *primitiveType(schema.UINT32)),
					newSchemaField("stamp", *primitiveType(schema.TIME)),
					newSchemaField("frame_id", *primitiveType(schema.STRING)),
				},
			})),
		},
		{
			"complex array",
			testutils.TrimLeadingSpace(`
			uint32 field1
			MyType[] field2
			===
			MSG: test/MyType
			string field3
			`),
			newSchema(
				newSchemaField("field1", *primitiveType(schema.UINT32)),
				newSchemaField("field2", schema.Type{
					Array: true,
					Items: &schema.Type{
						Record: true,
						Fields: []schema.Field{
							newSchemaField("field3", *primitiveType(schema.STRING)),
						},
					},
				}),
			),
		},
		{
			"subdependency with default value",
			testutils.TrimLeadingSpace(`
			MyType field1
			===
			MSG: test/MyType
			string field2 "foo"
			`),
			newSchema(
				newSchemaField("field1", schema.Type{
					Record: true,
					Fields: []schema.Field{
						newSchemaField("field2", *primitiveType(schema.STRING), "foo"),
					},
				}),
			),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			schema, err := ros2msg.ParseROS2MessageDefinition("test", "Test", []byte(c.msgdef))
			require.NoError(t, err)
			require.Equal(t, c.output, schema)
		})
	}
}

func TestTransformErrors(t *testing.T) {
	cases := []struct {
		assertion string
		msgdef    string
	}{
		{
			"invalid type",
			"foo bar",
		},
		{
			"invalid array size",
			"int32[foo] bar",
		},
		{
			"invalid array size",
			"int32[1.5] bar",
		},
		{
			"invalid array size",
			"int32[1.5] bar",
		},
		{
			"invalid array size",
			"int32[1.5] bar",
		},
		{
			"invalid array size",
			"int32[1.5] bar",
		},
		{
			"invalid array size",
			"int32[1.5] bar",
		},
		{
			"invalid array size",
			"int32[1.5] bar",
		},
		{
			"invalid array size",
			"int32[1.5] bar",
		},
		{
			"invalid array size",
			"int32[1.5] bar",
		},
		{
			"primitive resolution failure",
			"foo bar",
		},
		{
			"complex dependency resolution failure",
			testutils.TrimLeadingSpace(`
			Foo[] foo
			===
			MSG: test/Bar
			uint32 seq
			`),
		},
		{
			"complex dependency resolution failure x2",
			testutils.TrimLeadingSpace(`
			Foo[] foo
			===
			MSG: test/Foo
			Bar bar
			`),
		},
		{
			"invalid unscoped header type name",
			testutils.TrimLeadingSpace(`
			Foo[] foo
			===
			MSG: Foo
			Bar bar
			`),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			_, err := ros2msg.ParseROS2MessageDefinition("test", "Test", []byte(c.msgdef))
			require.Error(t, err)
		})
	}
}
