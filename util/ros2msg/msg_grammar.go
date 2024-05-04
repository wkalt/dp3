package ros2msg

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

/*
Grammar for the ROS2 IDL format:
https://docs.ros.org/en/iron/Concepts/Basic/About-Interfaces.html

This is for msg files only, no action or service support.
*/

// nolint:gochecknoglobals
var (
	Lexer = lexer.MustSimple([]lexer.SimpleRule{
		{Name: "Comment", Pattern: `#[^\n]*`},
		{Name: "Newline", Pattern: `\s*[\n\r]+`},
		{Name: "Float", Pattern: `[+-]?[0-9]+\.[0-9]+`},
		{Name: "Integer", Pattern: `[+-]?[0-9]+`},
		{Name: "QuotedString", Pattern: `"(\\"|[^"])*"`},
		{Name: "SingleQuotedString", Pattern: `'(\\'|[^'])*'`},
		{Name: "Word", Pattern: `[a-zA-Z0-9\_]+`},
		{Name: "Whitespace", Pattern: `[\s\t]+`},
		{Name: "LBracket", Pattern: `\[`},
		{Name: "RBracket", Pattern: `\]`},
		{Name: "Slash", Pattern: `/`},
		{Name: "Colon", Pattern: `:`},
		{Name: "LEQ", Pattern: `<=`},
		{Name: "Equals", Pattern: `=`},
	})

	MessageDefinitionParser = participle.MustBuild[MessageDefinition](
		participle.Lexer(Lexer),
		participle.Union[SchemaElement](Constant{}, ROSField{}),
		participle.Elide("Whitespace", "Newline", "Comment"),
		participle.UseLookahead(1000),
	)
)

type QuotedString string

func (q *QuotedString) Capture(values []string) error {
	v := values[0]
	*q = QuotedString(v[1 : len(v)-1])
	return nil
}

type MessageDefinition struct {
	Elements    []SchemaElement `@@*`
	Definitions []Definition    `@@*`
}

type Definition struct {
	Header   Header          `Equals+ @@`
	Elements []SchemaElement `@@*`
}

type Header struct {
	Type string `'MSG' Colon @(Word ( Slash Word )*)`
}

type ROSField struct {
	Type    *ROSType `@@`
	Name    string   `@Word`
	Default *Value   `@@?`
}

type Constant struct {
	Type  *ROSType `@@`
	Name  string   `@Word Equals`
	Value Value    `@@`
}

type Value struct {
	Int    *int64        `@Integer`
	Float  *float64      `| @Float`
	String *QuotedString `| (@QuotedString | @SingleQuotedString)`
}

func (v *Value) Value() any {
	if v.Int != nil {
		return *v.Int
	} else if v.Float != nil {
		return *v.Float
	} else {
		return string(*v.String)
	}
}

type ROSType struct {
	Name      string `@(Word ( Slash Word )*)`
	SizeBound int    `(LEQ @Integer)?`
	Array     bool   `@LBracket?`
	Bounded   bool   `@LEQ?`
	FixedSize int    `(( @Integer RBracket ) | RBracket)?`
}

type SchemaElement interface{ value() }

func (f ROSField) value() {}
func (c Constant) value() {}
