package ros1msg_test

import (
	"strings"
	"testing"

	"github.com/alecthomas/participle/v2"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/util/ros1msg"
)

func newField(name string, t *ros1msg.ROSType) *ros1msg.ROSField {
	return &ros1msg.ROSField{Name: name, Type: t}
}

func newHeader(t string) ros1msg.Header {
	return ros1msg.Header{Type: t}
}

func newDefinition(header ros1msg.Header, elements ...ros1msg.SchemaElement) *ros1msg.Definition {
	return &ros1msg.Definition{Header: header, Elements: elements}
}

func newMessageDefinition(
	elements []ros1msg.SchemaElement,
	deps ...ros1msg.Definition,
) *ros1msg.MessageDefinition {
	return &ros1msg.MessageDefinition{Elements: elements, Definitions: deps}
}

func newType(t string, array bool, fixedSize int) *ros1msg.ROSType {
	return &ros1msg.ROSType{
		Name:      t,
		Array:     array,
		FixedSize: fixedSize,
	}
}

func newConstant(name string, t *ros1msg.ROSType, value any) *ros1msg.Constant {
	c := &ros1msg.Constant{
		Type:  t,
		Name:  name,
		Value: ros1msg.ConstantValue{},
	}
	switch v := value.(type) {
	case string:
		c.Value.String = &v
	case int64:
		c.Value.Int = &v
	case float64:
		c.Value.Float = &v
	}
	return c
}

func TestMessageDefinitions(t *testing.T) {
	parser := participle.MustBuild[ros1msg.MessageDefinition](
		participle.Lexer(ros1msg.Lexer),
		participle.Union[ros1msg.SchemaElement](ros1msg.Constant{}, ros1msg.ROSField{}),
		participle.Elide("Whitespace", "Newline", "Comment"),
		participle.UseLookahead(10),
	)

	cases := []struct {
		assertion string
		input     string
		output    *ros1msg.MessageDefinition
	}{
		{
			"simple message definition",
			`int8 foo`,
			newMessageDefinition([]ros1msg.SchemaElement{*newField("foo", newType("int8", false, 0))}),
		},
		{
			"message definition that begins with a comment",
			strings.TrimSpace(`
# foo
int16 foo
				`),
			newMessageDefinition([]ros1msg.SchemaElement{
				*newField("foo", newType("int16", false, 0)),
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
			newMessageDefinition([]ros1msg.SchemaElement{
				*newField("foo", newType("int8", false, 0)),
			}, *newDefinition(newHeader("bar"), *newField("baz", newType("int16", false, 0))),
				*newDefinition(newHeader("qux"), *newField("quux", newType("int32", false, 0))),
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
	parser := participle.MustBuild[ros1msg.Definition](
		participle.Lexer(ros1msg.Lexer),
		participle.Union[ros1msg.SchemaElement](ros1msg.Constant{}, ros1msg.ROSField{}),
		participle.Elide("Whitespace", "Newline", "Comment"),
		participle.UseLookahead(1000),
	)
	cases := []struct {
		assertion string
		input     string
		output    *ros1msg.Definition
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
				*newField("seq", newType("uint32", false, 0)),
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
				*newConstant("NONE", newType("uint8", false, 0), int64(0)),
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
				*newConstant("NONE", newType("uint8", false, 0), int64(0)),
				*newField("bar", newType("int8", false, 0)),
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
	parser := participle.MustBuild[ros1msg.Header](
		participle.Lexer(ros1msg.Lexer),
		participle.Elide("Whitespace", "Newline", "Comment"),
	)
	cases := []struct {
		assertion string
		input     string
		output    ros1msg.Header
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
func TestField(t *testing.T) {
	parser := participle.MustBuild[ros1msg.ROSField](
		participle.Lexer(ros1msg.Lexer),
		participle.UseLookahead(1000),
		participle.Elide("Whitespace", "Newline", "Comment"),
	)
	cases := []struct {
		assertion string
		input     string
		output    ros1msg.SchemaElement
	}{
		{
			"indented field starting with comment",
			strings.TrimSpace(`
 # foo
 int8 foo
			`),
			newField("foo", newType("int8", false, 0)),
		},
		{
			"indented field starting with multiline indented comment",
			strings.TrimSpace(`
 # foo
 # foo
 int8 foo
			`),
			newField("foo", newType("int8", false, 0)),
		},
		{
			"simple field",
			"int8 foo",
			newField("foo", newType("int8", false, 0)),
		},
		{
			"field that starts with a space",
			" int8 foo",
			newField("foo", newType("int8", false, 0)),
		},
		{
			"field with a package",
			"my_package/Type foo",
			newField("foo", newType("my_package/Type", false, 0)),
		},
		{
			"field with inline comment",
			"int8 foo # foo comment",
			newField("foo", newType("int8", false, 0)),
		},
		{
			"comment multiple spaces",
			"int8 foo    # foo comment",
			newField("foo", newType("int8", false, 0)),
		},
		{
			"spaces after comment char",
			"int8 foo #     foo comment",
			newField("foo", newType("int8", false, 0)),
		},
		{
			"array field",
			"int8[] foo",
			newField("foo", newType("int8", true, 0)),
		},
		{
			"array field two digit",
			"int64[] foo",
			newField("foo", newType("int64", true, 0)),
		},
		{
			"fixed size array field",
			"int8[10] foo",
			newField("foo", newType("int8", true, 10)),
		},
		{
			"fixed size array with comment",
			"int8[10] foo # important comment",
			newField("foo", newType("int8", true, 10)),
		},
		{
			"fixed size array with multiline comment",
			strings.TrimSpace(`
# foo
# bar
int8[10] foo
					`),
			newField("foo", newType("int8", true, 10)),
		},
		{
			"field with comment that starts with space",
			strings.TrimSpace(`
 # foo
int8[10] foo
					`),
			newField("foo", newType("int8", true, 10)),
		},
		{
			"comment that uses punctuation",
			strings.TrimSpace(`
# Hello! I'm a comment. Can I answer any questions? :)
int8[10] foo
					`),
			newField("foo", newType("int8", true, 10)),
		},
		{
			"multiline leading comment with space",
			strings.TrimSpace(`
# foo
# bar

int8[10] foo
					`),
			newField("foo", newType("int8", true, 10)),
		},
		{
			"multiline leading comment internally spaced",
			strings.TrimSpace(`
# foo

# bar

int8[10] foo
					`),
			newField("foo", newType("int8", true, 10)),
		},
		{
			"multiline leading comment space on empty line",
			strings.TrimSpace(`
# foo
    
# bar

int8[10] foo
					`),
			newField("foo", newType("int8", true, 10)),
		},
		{
			"field with multiline inline comment",
			strings.TrimSpace(`
int8 foo # foo
		 # bar
			`),
			newField("foo", newType("int8", false, 0)),
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
	parser := participle.MustBuild[ros1msg.ROSType](
		participle.Lexer(ros1msg.Lexer),
		participle.Elide("Whitespace", "Newline", "Comment"),
	)
	cases := []struct {
		assertion string
		input     string
		output    *ros1msg.ROSType
	}{
		{
			"alpha type",
			"int",
			newType("int", false, 0),
		},
		{
			"alphanum type",
			"int8",
			newType("int8", false, 0),
		},
		{
			"complex type",
			"my_package/Foo",
			newType("my_package/Foo", false, 0),
		},
		{
			"variable sized array",
			"int8[]",
			newType("int8", true, 0),
		},
		{
			"fixed size",
			"int8[10]",
			newType("int8", true, 10),
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
	parser := participle.MustBuild[ros1msg.Constant](
		participle.Lexer(ros1msg.Lexer),
		participle.Elide("Whitespace", "Newline", "Comment"),
	)
	cases := []struct {
		assertion string
		input     string
		output    ros1msg.SchemaElement
	}{
		{
			"int constant",
			"uint16 FOO=1",
			newConstant("FOO", newType("uint16", false, 0), int64(1)),
		},
		{
			"constant separated with spaces",
			"uint16 FOO = 1",
			newConstant("FOO", newType("uint16", false, 0), int64(1)),
		},
		{
			"negative integer constant",
			"uint16 FOO = -1",
			newConstant("FOO", newType("uint16", false, 0), int64(-1)),
		},
		{
			"positive integer constant",
			"uint16 FOO = +1",
			newConstant("FOO", newType("uint16", false, 0), int64(1)),
		},
		{
			"negative float constant",
			"float32 FOO = -3.14",
			newConstant("FOO", newType("float32", false, 0), float64(-3.14)),
		},
		{
			"positive float constant",
			"float32 FOO = +3.14",
			newConstant("FOO", newType("float32", false, 0), float64(3.14)),
		},
		{
			"string constant",
			"string FOO=bar",
			newConstant("FOO", newType("string", false, 0), "bar"),
		},
		{
			"float constant",
			"float32 FOO=3.14",
			newConstant("FOO", newType("float32", false, 0), 3.14),
		},
		{
			"constant with inline comment",
			"float32 FOO=3.14 # note this",
			newConstant("FOO", newType("float32", false, 0), 3.14),
		},
		{
			"constant with leading comment",
			strings.TrimSpace(`
# announcement
float32 FOO=3.14 # note this`,
			),
			newConstant("FOO", newType("float32", false, 0), 3.14),
		},
		{
			"constant with multiline leading comment",
			strings.TrimSpace(`
# foo
# bar
float32 FOO=3.14 # note this`,
			),
			newConstant("FOO", newType("float32", false, 0), 3.14),
		},
		{
			"constant with multiline leading comment with break",
			strings.TrimSpace(`
# foo

# bar
float32 FOO=3.14 # note this`,
			),
			newConstant("FOO", newType("float32", false, 0), 3.14),
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
