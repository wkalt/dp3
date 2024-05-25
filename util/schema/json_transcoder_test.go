package schema_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/ros1msg"
	"github.com/wkalt/dp3/util/schema"
	"github.com/wkalt/dp3/util/testutils"
)

func TestJSONPrimitives(t *testing.T) {
	cases := []struct {
		assertion string
		msgdef    string
		msg       []byte
		expected  string
	}{
		{
			"byte",
			"byte foo",
			testutils.U8b(42),
			`{"foo":42}`,
		},
		{
			"char",
			"char foo",
			testutils.U8b(42),
			`{"foo":42}`,
		},
		{
			"float64",
			"float64 foo",
			testutils.F64b(3.14),
			`{"foo":3.14}`,
		},
		{
			"float32",
			"float32 foo",
			testutils.F32b(3.14),
			`{"foo":3.14}`,
		},
		{
			"bool",
			"bool foo",
			testutils.Boolb(true),
			`{"foo":true}`,
		},
		{
			"int8",
			"int8 foo",
			testutils.I8b(42),
			`{"foo":42}`,
		},
		{
			"int16",
			"int16 foo",
			testutils.I16b(42),
			`{"foo":42}`,
		},
		{
			"int32",
			"int32 foo",
			testutils.I32b(42),
			`{"foo":42}`,
		},
		{
			"int64",
			"int64 foo",
			testutils.I64b(42),
			`{"foo":42}`,
		},
		{
			"uint8",
			"uint8 foo",
			testutils.U8b(42),
			`{"foo":42}`,
		},
		{
			"uint16",
			"uint16 foo",
			testutils.U16b(42),
			`{"foo":42}`,
		},
		{
			"uint32",
			"uint32 foo",
			testutils.U32b(42),
			`{"foo":42}`,
		},
		{
			"uint64",
			"uint64 foo",
			testutils.U64b(42),
			`{"foo":42}`,
		},
		{
			"string",
			"string foo",
			testutils.PrefixedString("foo"),
			`{"foo":"foo"}`,
		},
		{
			"time",
			"time foo",
			testutils.U64b(42),
			`{"foo":42000000000}`,
		},
		{
			"duration",
			"duration foo",
			testutils.U64b(42),
			`{"foo":42000000000}`,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			decoder := ros1msg.NewDecoder(nil)
			parsed, err := ros1msg.ParseROS1MessageDefinition("test", "test", []byte(c.msgdef))
			require.NoError(t, err)
			transcoder := schema.NewJSONTranscoder(parsed, decoder)
			buf := &bytes.Buffer{}
			require.NoError(t, transcoder.Transcode(buf, c.msg))
			require.Equal(t, c.expected, buf.String())
		})
	}
}

func TestJSONTranscoder(t *testing.T) {
	cases := []struct {
		assertion string
		msgdef    string
		msg       []byte
		expected  string
	}{
		{
			"byte array",
			"uint8[] foo",
			testutils.PrefixedString("foo"),
			`{"foo":"Zm9v"}`,
		},
		{
			"flat record one field",
			"string foo",
			testutils.PrefixedString("foo"),
			`{"foo":"foo"}`,
		},
		{
			"flat record two fields",
			`string foo
			uint32 bar`,
			append(testutils.PrefixedString("foo"), testutils.U32b(42)...),
			`{"foo":"foo","bar":42}`,
		},
		{
			"array field",
			`string[] foo`,
			append(testutils.U32b(1), testutils.PrefixedString("foo")...),
			`{"foo":["foo"]}`,
		},
		{
			"empty array field",
			`string[] foo`,
			testutils.U32b(0),
			`{"foo":[]}`,
		},
		{
			"array field with multiple elements",
			`string[] foo`,
			testutils.Flatten(testutils.U32b(2), testutils.PrefixedString("foo"), testutils.PrefixedString("bar")),
			`{"foo":["foo","bar"]}`,
		},
		{
			"nested record",
			`
			string foo
			MyType bar
			===
			MSG: test/MyType
			string baz
			`,
			testutils.Flatten(testutils.PrefixedString("foo"), testutils.PrefixedString("baz")),
			`{"foo":"foo","bar":{"baz":"baz"}}`,
		},
		{
			"nested record with array",
			`
			string foo
			MyType[] bar
			===
			MSG: test/MyType
			string baz
			`,
			testutils.Flatten(testutils.PrefixedString("foo"), testutils.U32b(1), testutils.PrefixedString("baz")),
			`{"foo":"foo","bar":[{"baz":"baz"}]}`,
		},
		{
			"nested empty record",
			`
			string foo
			MyType bar
			===
			MSG: test/MyType
			`,
			testutils.PrefixedString("foo"),
			`{"foo":"foo","bar":{}}`,
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			decoder := ros1msg.NewDecoder(nil)
			parsed, err := ros1msg.ParseROS1MessageDefinition("test", "test", []byte(c.msgdef))
			require.NoError(t, err)
			transcoder := schema.NewJSONTranscoder(parsed, decoder)
			buf := &bytes.Buffer{}
			require.NoError(t, transcoder.Transcode(buf, c.msg))
			require.Equal(t, c.expected, buf.String())
		})
	}
}
