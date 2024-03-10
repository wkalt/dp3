package ros1msg

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// nolint:gochecknoglobals
var (
	Lexer = lexer.MustSimple([]lexer.SimpleRule{
		{Name: "Comment", Pattern: `#[^\n]*`},
		{Name: "Newline", Pattern: `\s*[\n\r]+`},
		{Name: "Float", Pattern: `[+-]?[0-9]+\.[0-9]+`},
		{Name: "Integer", Pattern: `[+-]?[0-9]+`},
		{Name: "Word", Pattern: `[a-zA-Z0-9\_]+`},
		{Name: "Whitespace", Pattern: `[\s\t]+`},
		{Name: "LBracket", Pattern: `\[`},
		{Name: "RBracket", Pattern: `\]`},
		{Name: "Slash", Pattern: `/`},
		{Name: "Colon", Pattern: `:`},
		{Name: "Equals", Pattern: `=`},
	})

	MessageDefinitionParser = participle.MustBuild[MessageDefinition](
		participle.Lexer(Lexer),
		participle.Union[SchemaElement](Constant{}, ROSField{}),
		// NB: parsing comments would be great, but it is difficult to infer
		// what field a comment should attach to - so for now we elide.
		participle.Elide("Whitespace", "Newline", "Comment"),
		participle.UseLookahead(1000),
	)
)

type MessageDefinition struct {
	Elements    []SchemaElement `parser:"@@*"`
	Definitions []Definition    `parser:"@@*"`
}

type Definition struct {
	Header   Header          `parser:"Equals+ @@"`
	Elements []SchemaElement `parser:"@@*"`
}
type Header struct {
	Type string `parser:"'MSG' Colon @(Word ( Slash Word )*)"`
}

type ROSField struct {
	Type *ROSType `parser:"@@"`
	Name string   `parser:"@Word"`
}

type Constant struct {
	Type  *ROSType      `parser:"@@"`
	Name  string        `parser:"@Word Equals"`
	Value ConstantValue `parser:"@@"`
}

type ConstantValue struct {
	String *string  `parser:"@Word"`
	Int    *int64   `parser:"| @Integer"`
	Float  *float64 `parser:"| @Float"`
}

type ROSType struct {
	Name      string `parser:"@(Word ( Slash Word )*)"`
	Array     bool   `parser:"@LBracket?"`
	FixedSize int    `parser:"(( @Integer RBracket ) | RBracket)?"`
}

type SchemaElement interface{ value() }

func (f ROSField) value() {}
func (c Constant) value() {}
