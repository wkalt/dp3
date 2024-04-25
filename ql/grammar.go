package ql

import (
	"fmt"
	"strconv"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/relvacode/iso8601"
)

/*
This file contains a participle grammar for the dp3 query language. The language
is intended to provide a simple and ergonomic mechanism for executing as-of
joins.
*/

////////////////////////////////////////////////////////////////////////////////

var (
	Options = []participle.Option{ // nolint:gochecknoglobals
		participle.Lexer(
			lexer.MustSimple([]lexer.SimpleRule{
				{Name: "Word", Pattern: `[a-zA-Z_/\.][a-zA-Z0-9_/\.-]*`},
				{Name: "QuotedString", Pattern: `"(?:\\.|[^"])*"`},
				{Name: "whitespace", Pattern: `\s+`},
				{Name: "Operators", Pattern: `,|[()]|;`},
				{Name: "BinaryOperator", Pattern: `=|!=|<=|>=|<|>|~\*|~`},
				{Name: "Float", Pattern: `[-+]?\d*\.\d+([eE][-+]?\d+)?`},
				{Name: "Integer", Pattern: `[0-9]+([eE][-+]?\d+)?`},
			}),
		),
		participle.Unquote("QuotedString"),
	}
)

// Query represents a query in the dp3 query language.
type Query struct {
	From         string       `"from" @Word`
	Between      *Between     `@@?`
	Select       Select       `@@`
	Where        *Expression  `("where" @@)*`
	Descending   bool         `@"desc"?`
	PagingClause []PagingTerm `@@*`
	Terminator   string       `";"`
}

type Term struct {
	Subexpression *Expression ` "(" @@ ")"`
	Value         *string     `| @Word`
}

type Expression struct {
	Or []*OrCondition `@@ ( "or" @@ )*`
}

type OrCondition struct {
	And []*Condition `@@ ( "and" @@ )*`
}

type Condition struct {
	Operand Term          `@@`
	RHS     *ConditionRHS `@@?`
}

type ConditionRHS struct {
	Op    string `@BinaryOperator`
	Value Value  `@@`
}

// Between represents a time range.
type Between struct {
	From Timestamp `"between" @@ "and"`
	To   Timestamp `@@`
}

// Select represents a select statement.
type Select struct {
	Entity string `@Word`
	Alias  string `("as" @Word)?`
	MJ     *MJ    `( @@`
	AJ     *AJ    `| @@ )?`
}

// MJ represents a merge join.
type MJ struct {
	Select Select `("," @@)`
}

// Timestamp represents a timestamp.
type Timestamp struct {
	Nanoseconds *int64  `( @Integer`
	Datestring  *string `| @QuotedString )`
}

// Nanos returns the timestamp in nanoseconds.
func (t Timestamp) Nanos() (int64, error) {
	if t.Nanoseconds != nil {
		return *t.Nanoseconds, nil
	}
	time, err := iso8601.Parse([]byte(*t.Datestring))
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp: %w", err)
	}
	return time.UnixNano(), nil
}

// Value represents a value.
type Value struct {
	Text    *string  `@QuotedString`
	Integer *int64   `| @Integer`
	Bool    *bool    `| @("true" | "false")`
	Float   *float64 `| @Float`
}

func (v Value) Value() any {
	if v.Text != nil {
		return *v.Text
	}
	if v.Integer != nil {
		return *v.Integer
	}
	if v.Float != nil {
		return *v.Float
	}
	panic("invalid value")
}

// String returns the string representation of the value.
func (v Value) String() string {
	if v.Text != nil {
		return *v.Text
	}
	if v.Integer != nil {
		return strconv.FormatInt(*v.Integer, 10)
	}
	if v.Float != nil {
		return fmt.Sprintf("%f", *v.Float)
	}
	panic("invalid value")
}

// AJ represents an as-of join.
type AJ struct {
	Keyword    string          `@( "precedes" | "succeeds" | "neighbors" )`
	Immediate  bool            `@("immediate")?`
	Select     Select          `@@`
	Constraint *AsOfConstraint `(@@)*`
}

// AsOfConstraint represents an as-of constraint.
type AsOfConstraint struct {
	Quantity int    `("by" "less" "than" @Integer )`
	Units    string `@("nanoseconds" | "microseconds" | "milliseconds" | "seconds" | "minutes")`
}

// PagingTerm represents a limit/offset term.
type PagingTerm struct {
	Keyword string `@("limit" | "offset")`
	Value   int    `@Integer`
}

// NewParser returns a new query parser.
func NewParser() *participle.Parser[Query] {
	return participle.MustBuild[Query](Options...)
}
