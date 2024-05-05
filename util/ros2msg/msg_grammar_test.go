package ros2msg_test

import (
	"strings"
	"testing"

	"github.com/alecthomas/participle/v2"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/ros2msg"
)

func newField(name string, t *ros2msg.ROSType, dfault *ros2msg.Value) *ros2msg.ROSField {
	return &ros2msg.ROSField{Name: name, Type: t, Default: dfault}
}

func newHeader(t string) ros2msg.Header {
	return ros2msg.Header{Type: t}
}

func newDefinition(header ros2msg.Header, elements ...ros2msg.SchemaElement) *ros2msg.Definition {
	return &ros2msg.Definition{Header: header, Elements: elements}
}

func newMessageDefinition(
	elements []ros2msg.SchemaElement,
	deps ...ros2msg.Definition,
) *ros2msg.MessageDefinition {
	return &ros2msg.MessageDefinition{Elements: elements, Definitions: deps}
}

func newType(t string, sizeBound int, array bool, bounded bool, fixedSize int) *ros2msg.ROSType {
	return &ros2msg.ROSType{
		Name:      t,
		SizeBound: sizeBound,
		Array:     array,
		Bounded:   bounded,
		FixedSize: fixedSize,
	}
}

func newValue(v any) *ros2msg.Value {
	value := &ros2msg.Value{
		String: nil,
		Int:    nil,
		Float:  nil,
	}
	switch v := v.(type) {
	case string:
		s := ros2msg.QuotedString(v)
		value.String = &s
	case int64:
		value.Int = &v
	case float64:
		value.Float = &v
	default:
		panic("unknown value type")
	}
	return value
}

func newConstant(name string, t *ros2msg.ROSType, value any) *ros2msg.Constant {
	c := &ros2msg.Constant{
		Type:  t,
		Name:  name,
		Value: *newValue(value),
	}
	return c
}

func TestMessageDefinitions(t *testing.T) {
	parser := participle.MustBuild[ros2msg.MessageDefinition](
		participle.Lexer(ros2msg.Lexer),
		participle.Union[ros2msg.SchemaElement](ros2msg.Constant{}, ros2msg.ROSField{}),
		participle.Elide("Whitespace", "Newline", "Comment"),
		participle.UseLookahead(10),
	)

	cases := []struct {
		assertion string
		input     string
		output    *ros2msg.MessageDefinition
	}{
		{
			"simple message definition",
			`int8 foo`,
			newMessageDefinition([]ros2msg.SchemaElement{*newField("foo", newType("int8", 0, false, false, 0), nil)}),
		},
		{
			"message definition that begins with a comment",
			strings.TrimSpace(`
# foo
int16 foo
				`),
			newMessageDefinition([]ros2msg.SchemaElement{
				*newField("foo", newType("int16", 0, false, false, 0), nil),
			}),
		},
		{
			"definition with dependencies",
			strings.TrimSpace(`
int8 foo
===
MSG: bar
int16 baz
===
MSG: qux
int32 quux
					`),
			newMessageDefinition([]ros2msg.SchemaElement{
				*newField("foo", newType("int8", 0, false, false, 0), nil),
			}, *newDefinition(newHeader("bar"), *newField("baz", newType("int16", 0, false, false, 0), nil)),
				*newDefinition(newHeader("qux"), *newField("quux", newType("int32", 0, false, false, 0), nil)),
			),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.output, ast)
		})
	}
}

func TestDefinition(t *testing.T) {
	parser := participle.MustBuild[ros2msg.Definition](
		participle.Lexer(ros2msg.Lexer),
		participle.Union[ros2msg.SchemaElement](ros2msg.Constant{}, ros2msg.ROSField{}),
		participle.Elide("Whitespace", "Newline", "Comment"),
		participle.UseLookahead(1000),
	)
	cases := []struct {
		assertion string
		input     string
		output    *ros2msg.Definition
	}{
		{
			"comment under header",
			strings.TrimSpace(`
===
MSG: std_msgs/Header
# foo
uint32 seq
					`),
			newDefinition(newHeader("std_msgs/Header"),
				*newField("seq", newType("uint32", 0, false, false, 0), nil),
			),
		},
		{
			"definition with no fields",
			strings.TrimSpace(`
===
MSG: std_msgs/Header
					`),
			newDefinition(newHeader("std_msgs/Header")),
		},
		{
			"message definition with constants",
			strings.TrimSpace(`
======
MSG: foo
uint8 NONE=0
						`),
			newDefinition(newHeader("foo"),
				*newConstant("NONE", newType("uint8", 0, false, false, 0), int64(0)),
			),
		},
		{
			"mix of fields and constants",
			strings.TrimSpace(`
======
MSG: foo
uint8 NONE=0
int8 bar
				`),
			newDefinition(newHeader("foo"),
				*newConstant("NONE", newType("uint8", 0, false, false, 0), int64(0)),
				*newField("bar", newType("int8", 0, false, false, 0), nil),
			),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.output, ast)
		})
	}
}

func TestHeader(t *testing.T) {
	parser := participle.MustBuild[ros2msg.Header](
		participle.Lexer(ros2msg.Lexer),
		participle.Elide("Whitespace", "Newline", "Comment"),
	)
	cases := []struct {
		assertion string
		input     string
		output    ros2msg.Header
	}{
		{
			"simple header",
			"MSG: foo",
			newHeader("foo"),
		},
		{
			"type with a slash",
			"MSG: foo/bar",
			newHeader("foo/bar"),
		},
		{
			"type with a nesting",
			"MSG: foo/bar/baz",
			newHeader("foo/bar/baz"),
		},
		{
			"header with inline comment",
			"MSG: foo/bar/baz # hello",
			newHeader("foo/bar/baz"),
		},
		{
			"comment without a space",
			"MSG: foo/bar/baz #hello",
			newHeader("foo/bar/baz"),
		},
		{
			"header with leading comment",
			strings.TrimSpace(`
# warning
MSG: foo/bar/baz # hello
			`),
			newHeader("foo/bar/baz"),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.output, *ast)
		})
	}
}

func TestValue(t *testing.T) {
	parser := participle.MustBuild[ros2msg.Value](
		participle.Lexer(ros2msg.Lexer),
		participle.Elide("Whitespace", "Newline", "Comment"),
	)
	cases := []struct {
		assertion string
		input     string
		output    ros2msg.Value
	}{
		{
			"integer value",
			"1",
			*newValue(int64(1)),
		},
		{
			"float value",
			"3.14",
			*newValue(3.14),
		},
		{
			"single quoted string",
			`'foo'`,
			*newValue("foo"),
		},
		{
			"double quoted string",
			`"foo"`,
			*newValue("foo"),
		},
		{
			"string with spaces",
			`"foo bar baz"`,
			*newValue("foo bar baz"),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.output, *ast)
		})
	}
}

func TestField(t *testing.T) {
	parser := participle.MustBuild[ros2msg.ROSField](
		participle.Lexer(ros2msg.Lexer),
		participle.UseLookahead(1000),
		participle.Elide("Whitespace", "Newline", "Comment"),
	)
	cases := []struct {
		assertion string
		input     string
		output    ros2msg.SchemaElement
	}{
		{
			"indented field starting with comment",
			strings.TrimSpace(`
 # foo
 int8 foo
			`),
			newField("foo", newType("int8", 0, false, false, 0), nil),
		},
		{
			"field with default value",
			"int8 foo 10",
			newField("foo", newType("int8", 0, false, false, 0), newValue(int64(10))),
		},
		{
			"field with quoted string value",
			`string foo 'bar'`,
			newField("foo", newType("string", 0, false, false, 0), newValue("bar")),
		},
		{
			"indented field starting with multiline indented comment",
			strings.TrimSpace(`
 # foo
 # foo
 int8 foo
			`),
			newField("foo", newType("int8", 0, false, false, 0), nil),
		},
		{
			"simple field",
			"int8 foo",
			newField("foo", newType("int8", 0, false, false, 0), nil),
		},
		{
			"field that starts with a space",
			" int8 foo",
			newField("foo", newType("int8", 0, false, false, 0), nil),
		},
		{
			"field with a package",
			"my_package/Type foo",
			newField("foo", newType("my_package/Type", 0, false, false, 0), nil),
		},
		{
			"field with inline comment",
			"int8 foo # foo comment",
			newField("foo", newType("int8", 0, false, false, 0), nil),
		},
		{
			"comment multiple spaces",
			"int8 foo    # foo comment",
			newField("foo", newType("int8", 0, false, false, 0), nil),
		},
		{
			"spaces after comment char",
			"int8 foo #     foo comment",
			newField("foo", newType("int8", 0, false, false, 0), nil),
		},
		{
			"array field",
			"int8[] foo",
			newField("foo", newType("int8", 0, true, false, 0), nil),
		},
		{
			"array field two digit",
			"int64[] foo",
			newField("foo", newType("int64", 0, true, false, 0), nil),
		},
		{
			"fixed size array field",
			"int8[10] foo",
			newField("foo", newType("int8", 0, true, false, 10), nil),
		},
		{
			"fixed size array with comment",
			"int8[10] foo # important comment",
			newField("foo", newType("int8", 0, true, false, 10), nil),
		},
		{
			"fixed size array with multiline comment",
			strings.TrimSpace(`
# foo
# bar
int8[10] foo
					`),
			newField("foo", newType("int8", 0, true, false, 10), nil),
		},
		{
			"field with comment that starts with space",
			strings.TrimSpace(`
 # foo
int8[10] foo
					`),
			newField("foo", newType("int8", 0, true, false, 10), nil),
		},
		{
			"comment that uses punctuation",
			strings.TrimSpace(`
# Hello! I'm a comment. Can I answer any questions? :)
int8[10] foo
					`),
			newField("foo", newType("int8", 0, true, false, 10), nil),
		},
		{
			"multiline leading comment with space",
			strings.TrimSpace(`
# foo
# bar

int8[10] foo
					`),
			newField("foo", newType("int8", 0, true, false, 10), nil),
		},
		{
			"multiline leading comment internally spaced",
			strings.TrimSpace(`
# foo

# bar

int8[10] foo
					`),
			newField("foo", newType("int8", 0, true, false, 10), nil),
		},
		{
			"multiline leading comment space on empty line",
			strings.TrimSpace(`
# foo
    
# bar

int8[10] foo
					`),
			newField("foo", newType("int8", 0, true, false, 10), nil),
		},
		{
			"field with multiline inline comment",
			strings.TrimSpace(`
int8 foo # foo
		 # bar
			`),
			newField("foo", newType("int8", 0, false, false, 0), nil),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.output, ast)
		})
	}
}

func TestType(t *testing.T) {
	parser := participle.MustBuild[ros2msg.ROSType](
		participle.Lexer(ros2msg.Lexer),
		participle.Elide("Whitespace", "Newline", "Comment"),
	)
	cases := []struct {
		assertion string
		input     string
		output    *ros2msg.ROSType
	}{
		{
			"alpha type",
			"int",
			newType("int", 0, false, false, 0),
		},
		{
			"alphanum type",
			"int8",
			newType("int8", 0, false, false, 0),
		},
		{
			"complex type",
			"my_package/Foo",
			newType("my_package/Foo", 0, false, false, 0),
		},
		{
			"unbounded array",
			"int8[]",
			newType("int8", 0, true, false, 0),
		},
		{
			"static array",
			"int8[10]",
			newType("int8", 0, true, false, 10),
		},
		{
			"bounded array",
			"int8[<=10]",
			newType("int8", 0, true, true, 10),
		},
		{
			"bounded length field",
			"string<=10",
			newType("string", 10, false, false, 0),
		},
		{
			"bounded length field with array",
			"string<=10[]",
			newType("string", 10, true, false, 0),
		},
		{
			"bounded length field with array and fixed size",
			"string<=10[10]",
			newType("string", 10, true, false, 10),
		},
		{
			"bounded length field with bounded size",
			"string<=10[<=10]",
			newType("string", 10, true, true, 10),
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.output, ast)
		})
	}
}

func TestConstant(t *testing.T) {
	parser := participle.MustBuild[ros2msg.Constant](
		participle.Lexer(ros2msg.Lexer),
		participle.Elide("Whitespace", "Newline", "Comment"),
	)
	cases := []struct {
		assertion string
		input     string
		output    ros2msg.SchemaElement
	}{
		{
			"int constant",
			"uint16 FOO=1",
			newConstant("FOO", newType("uint16", 0, false, false, 0), int64(1)),
		},
		{
			"constant separated with spaces",
			"uint16 FOO = 1",
			newConstant("FOO", newType("uint16", 0, false, false, 0), int64(1)),
		},
		{
			"negative integer constant",
			"uint16 FOO = -1",
			newConstant("FOO", newType("uint16", 0, false, false, 0), int64(-1)),
		},
		{
			"positive integer constant",
			"uint16 FOO = +1",
			newConstant("FOO", newType("uint16", 0, false, false, 0), int64(1)),
		},
		{
			"negative float constant",
			"float32 FOO = -3.14",
			newConstant("FOO", newType("float32", 0, false, false, 0), float64(-3.14)),
		},
		{
			"positive float constant",
			"float32 FOO = +3.14",
			newConstant("FOO", newType("float32", 0, false, false, 0), float64(3.14)),
		},
		{
			"string constant",
			`string FOO="bar"`,
			newConstant("FOO", newType("string", 0, false, false, 0), "bar"),
		},
		{
			"float constant",
			"float32 FOO=3.14",
			newConstant("FOO", newType("float32", 0, false, false, 0), 3.14),
		},
		{
			"constant with inline comment",
			"float32 FOO=3.14 # note this",
			newConstant("FOO", newType("float32", 0, false, false, 0), 3.14),
		},
		{
			"constant with leading comment",
			strings.TrimSpace(`
# announcement
float32 FOO=3.14 # note this`,
			),
			newConstant("FOO", newType("float32", 0, false, false, 0), 3.14),
		},
		{
			"constant with multiline leading comment",
			strings.TrimSpace(`
# foo
# bar
float32 FOO=3.14 # note this`,
			),
			newConstant("FOO", newType("float32", 0, false, false, 0), 3.14),
		},
		{
			"constant with multiline leading comment with break",
			strings.TrimSpace(`
# foo

# bar
float32 FOO=3.14 # note this`,
			),
			newConstant("FOO", newType("float32", 0, false, false, 0), 3.14),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.output, ast)
		})
	}
}
