package ros1msg_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/ros1msg"
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
			schema, err := ros1msg.ParseROS1MessageDefinition("test", "Test", []byte(c.msgdef))
			require.NoError(t, err)
			require.Equal(t, c.output, schema)
		})
	}
}

func TestParseROS1MessageDefinitions(t *testing.T) {
	require.NoError(t, filepath.WalkDir("testdata/schemas", func(
		path string,
		d fs.DirEntry,
		err error,
	) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		bytes, err := os.ReadFile(path)
		require.NoError(t, err)
		parts := strings.Split(d.Name(), "-")
		pkg := parts[0]
		name := strings.TrimSuffix(parts[1], ".msg")
		_, err = ros1msg.ParseROS1MessageDefinition(pkg, name, bytes)
		require.NoError(t, err, "failed to parse %s", path)
		return nil
	}))
}
