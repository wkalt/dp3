package ros2msg_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/ros2msg"
	"github.com/wkalt/dp3/util/schema"
)

func primitiveType(t schema.PrimitiveType) *schema.Type {
	return &schema.Type{
		Primitive: t,
	}
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
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "foo",
						Type: *primitiveType(schema.STRING),
					},
				},
			},
		},
		{
			"primitive with default value",
			`string foo "bar"`,
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name:    "foo",
						Type:    *primitiveType(schema.STRING),
						Default: "bar",
					},
				},
			},
		},
		{
			"primitive with integer default value",
			`int32 foo 42`,
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name:    "foo",
						Type:    *primitiveType(schema.INT32),
						Default: int64(42),
					},
				},
			},
		},
		{
			"primitive with float default value",
			`float32 foo 3.14`,
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name:    "foo",
						Type:    *primitiveType(schema.FLOAT32),
						Default: float64(3.14),
					},
				},
			},
		},
		{
			"primitive array",
			"string[10] foo",
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "foo",
						Type: schema.Type{
							Array:     true,
							Items:     primitiveType(schema.STRING),
							FixedSize: 10,
						},
					},
				},
			},
		},
		{
			"bounded length array",
			"string[<=10] foo",
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "foo",
						Type: schema.Type{
							Array:     true,
							Items:     primitiveType(schema.STRING),
							FixedSize: 10,

							Bounded: true,
						},
					},
				},
			},
		},
		{
			"size bounded field with bounded length array",
			"string<=10[<=10] foo",
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "foo",
						Type: schema.Type{
							Array:     true,
							Items:     primitiveType(schema.STRING),
							FixedSize: 10,

							Bounded:   true,
							SizeBound: 10,
						},
					},
				},
			},
		},
		{
			"bounded length string field",
			"string<=10 foo",
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "foo",
						Type: schema.Type{
							Primitive: schema.STRING,
							SizeBound: 10,
						},
					},
				},
			},
		},
		{
			"primitive array",
			"string[10] foo",
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "foo",
						Type: schema.Type{
							Array:     true,
							Items:     primitiveType(schema.STRING),
							FixedSize: 10,
						},
					},
				},
			},
		},
		{
			"subdependencies",
			strings.TrimSpace(`
Header header #for timestamp                                                               
===
MSG: std_msgs/Header                                                                       
uint32 seq                                                                                 
time stamp                                                                                 
string frame_id
`),
			&schema.Schema{
				Name: "test/Test",
				Fields: []schema.Field{
					{
						Name: "header",
						Type: schema.Type{
							Record: true,
							Fields: []schema.Field{
								{
									Name: "seq",
									Type: *primitiveType(schema.UINT32),
								},
								{
									Name: "stamp",
									Type: *primitiveType(schema.TIME),
								},
								{
									Name: "frame_id",
									Type: *primitiveType(schema.STRING),
								},
							},
						},
					},
				},
			},
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
