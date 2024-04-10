package ql_test

import (
	"testing"

	"github.com/alecthomas/participle/v2"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/query/ql"
	"github.com/wkalt/dp3/util"
)

const (
	producer = "my-robot"
)

func TestValue(t *testing.T) {
	parser, err := participle.Build[ql.Value](ql.Options...)
	require.NoError(t, err)
	cases := []struct {
		assertion string
		input     string
		expected  *ql.Value
	}{
		{
			"integer",
			"10",
			newValue(int64(10)),
		},
		{
			"float",
			"10.5",
			newValue(float64(10.5)),
		},
		{
			"long float",
			"3.141592653589793",
			newValue(float64(3.141592653589793)),
		},
		{
			"scientific notation", // todo - custom int parser for scientific notation ints.
			"1.0e6",
			newValue(float64(1e6)),
		},
		{
			"string",
			`"a"`,
			newValue("a"),
		},
		{
			"string starting with a dot",
			`".a"`,
			newValue(".a"),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, ast)
		})
	}
}

func TestOrClause(t *testing.T) {
	parser := participle.MustBuild[ql.OrClause](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  ql.OrClause
	}{
		{
			"single condition",
			"a = 10",
			newOr(newBinaryExpr("a", "=", int64(10))),
		},
		{
			"dotted path",
			"a.b = 10",
			newOr(newBinaryExpr("a.b", "=", int64(10))),
		},
		{
			"path starting with a dot",
			".a = 10",
			newOr(newBinaryExpr(".a", "=", int64(10))),
		},
		{
			"multiple dots",
			"a.b.baz = 10",
			newOr(newBinaryExpr("a.b.baz", "=", int64(10))),
		},
		{
			"multiple conditions",
			"a = 10 and b = 20",
			newOr(
				newBinaryExpr("a", "=", int64(10)),
				newBinaryExpr("b", "=", int64(20)),
			),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, *ast)
		})
	}
}

func TestBinaryExpression(t *testing.T) {
	parser := participle.MustBuild[ql.BinaryExpression](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  ql.BinaryExpression
	}{
		{
			"equality",
			"a = 10",
			newBinaryExpr("a", "=", int64(10)),
		},
		{
			"inequality",
			"a != 10",
			newBinaryExpr("a", "!=", int64(10)),
		},
		{
			"greater than",
			"a > 10",
			newBinaryExpr("a", ">", int64(10)),
		},
		{
			"greater than or equal",
			"a >= 10",
			newBinaryExpr("a", ">=", int64(10)),
		},
		{
			"less than",
			"a < 10",
			newBinaryExpr("a", "<", int64(10)),
		},
		{
			"less than or equal",
			"a <= 10",
			newBinaryExpr("a", "<=", int64(10)),
		},
		{
			"regex",
			`a ~ "b"`,
			newBinaryExpr("a", "~", "b"),
		},
		{
			"case insensitive regex",
			`a ~* "b"`,
			newBinaryExpr("a", "~*", "b"),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, *ast)
		})
	}
}

func TestMJ(t *testing.T) {
	parser := participle.MustBuild[ql.MJ](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  *ql.MJ
	}{
		{
			"single",
			", a",
			newMJ(newSelect("a", "", nil, nil)),
		},
		{
			"multiple",
			", a, b",
			newMJ(newSelect("a", "", newMJ(newSelect("b", "", nil, nil)), nil)),
		},
		{
			"nested",
			", a, b, c",
			newMJ(newSelect("a", "",
				newMJ(newSelect("b", "",
					newMJ(newSelect("c", "", nil, nil)), nil)), nil)),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, ast)
		})
	}
}

func TestAJ(t *testing.T) {
	parser := participle.MustBuild[ql.AJ](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  *ql.AJ
	}{
		{
			"precedes",
			"precedes b",
			newAJ("precedes", false, newSelect("b", "", nil, nil), nil),
		},
		{
			"precedes immediate",
			"precedes immediate b",
			newAJ("precedes", true, newSelect("b", "", nil, nil), nil),
		},
		{
			"succeeds",
			"succeeds b",
			newAJ("succeeds", false, newSelect("b", "", nil, nil), nil),
		},
		{
			"neighbors",
			"neighbors b",
			newAJ("neighbors", false, newSelect("b", "", nil, nil), nil),
		},
		{
			"precedes with constraint",
			"precedes b by less than 20 seconds",
			newAJ("precedes", false, newSelect("b", "", nil, nil), newAsofConstraint(20, "seconds")),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, ast)
		})
	}
}

func TestPagingTerm(t *testing.T) {
	parser := participle.MustBuild[ql.PagingTerm](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  ql.PagingTerm
	}{
		{
			"limit",
			"limit 10",
			newPagingTerm("limit", 10),
		},
		{
			"offset",
			"offset 10",
			newPagingTerm("offset", 10),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, *ast)
		})
	}
}

func TestSelect(t *testing.T) {
	parser := participle.MustBuild[ql.Select](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  ql.Select
	}{
		{
			"simple",
			"a",
			newSelect("a", "", nil, nil),
		},
		{
			"aliased",
			"a as b",
			newSelect("a", "b", nil, nil),
		},
		{
			"multiple",
			"a, b",
			newSelect("a", "", newMJ(newSelect("b", "", nil, nil)), nil),
		},
		{
			"multiple aliased",
			"a as foo, b as bar",
			newSelect("a", "foo", newMJ(newSelect("b", "bar", nil, nil)), nil),
		},
		{
			"multiple with constraint",
			"a precedes b by less than 10 nanoseconds",
			newSelect("a", "", nil,
				newAJ("precedes", false,
					newSelect("b", "", nil, nil),
					newAsofConstraint(10, "nanoseconds"),
				)),
		},
		{
			"aj with aliases",
			"a as foo precedes b as bar by less than 10 nanoseconds",
			newSelect("a", "foo", nil,
				newAJ("precedes", false,
					newSelect("b", "bar", nil, nil),
					newAsofConstraint(10, "nanoseconds"),
				)),
		},
		{
			"mixed merge and as-of",
			"a, b precedes c by less than 10 nanoseconds",
			newSelect("a", "",
				newMJ(
					newSelect("b", "", nil,
						newAJ("precedes", false,
							newSelect("c", "", nil, nil),
							newAsofConstraint(10, "nanoseconds")))), nil),
		},
		{
			"aj before mj",
			"a precedes b, c",
			newSelect("a", "", nil,
				newAJ("precedes", false,
					newSelect("b", "",
						newMJ(
							newSelect("c", "", nil, nil)), nil), nil)),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, *ast)
		})
	}
}

func TestAsofConstraint(t *testing.T) {
	parser := participle.MustBuild[ql.AsOfConstraint](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  *ql.AsOfConstraint
	}{
		{
			"nanoseconds",
			"by less than 10 nanoseconds",
			newAsofConstraint(10, "nanoseconds"),
		},
		{
			"microseconds",
			"by less than 10 microseconds",
			newAsofConstraint(10, "microseconds"),
		},
		{
			"milliseconds",
			"by less than 10 milliseconds",
			newAsofConstraint(10, "milliseconds"),
		},
		{
			"seconds",
			"by less than 10 seconds",
			newAsofConstraint(10, "seconds"),
		},
		{
			"minutes",
			"by less than 10 minutes",
			newAsofConstraint(10, "minutes"),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, ast)
		})
	}
}

func TestBetween(t *testing.T) {
	parser := participle.MustBuild[ql.Between](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  *ql.Between
	}{
		{
			"strings",
			`between "a" and "b"`,
			newBetween("a", "b"),
		},
		{
			"integers",
			"between 10 and 20",
			newBetween(10, 20),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, ast)
		})
	}
}

func TestQuery(t *testing.T) {
	parser := participle.MustBuild[ql.Query](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  ql.Query
	}{
		{
			"simple",
			"from my-robot a",
			newQuery(nil, newSelect("a", "", nil, nil), nil, nil),
		},
		{
			"with between",
			`from my-robot between "a" and "b" a`,
			newQuery(newBetween("a", "b"), newSelect("a", "", nil, nil), nil, nil),
		},
		{
			"with where",
			"from my-robot a where a = 10",
			newQuery(
				nil,
				newSelect("a", "", nil, nil),
				newWhere(newOr(newBinaryExpr("a", "=", int64(10)))),
				nil,
			),
		},
		{
			"with where and paging",
			"from my-robot a where a = 10 limit 10 offset 10",
			newQuery(
				nil,
				newSelect("a", "", nil, nil),
				newWhere(newOr(newBinaryExpr("a", "=", int64(10)))),
				newPagingClause("limit", 10, "offset", 10),
			),
		},
		{
			"with multiple where",
			"from my-robot a where a = 10 and b = 20",
			newQuery(
				nil,
				newSelect("a", "", nil, nil),
				newWhere(
					newOr(
						newBinaryExpr("a", "=", int64(10)),
						newBinaryExpr("b", "=", int64(20)),
					),
				),
				nil,
			),
		},
		{
			"merge join with where clauses",
			"from my-robot a, b where a = 10 and b = 20",
			newQuery(
				nil,
				newSelect("a", "", newMJ(newSelect("b", "", nil, nil)), nil),
				newWhere(
					newOr(
						newBinaryExpr("a", "=", int64(10)),
						newBinaryExpr("b", "=", int64(20)),
					),
				),
				nil,
			),
		},
		{
			"basic as-of join",
			"from my-robot a precedes b by less than 10 seconds",
			newQuery(
				nil,
				newSelect("a", "", nil, newAJ("precedes", false,
					newSelect("b", "", nil, nil),
					newAsofConstraint(10, "seconds"))),
				nil,
				nil,
			),
		},
		{
			"as-of join without constraint",
			"from my-robot a precedes b",
			newQuery(
				nil,
				newSelect("a", "", nil, newAJ("precedes", false, newSelect("b", "", nil, nil), nil)),
				nil,
				nil,
			),
		},
		{
			"as-of join without constraint with limit and offset",
			"from my-robot a precedes b limit 10 offset 10",
			newQuery(
				nil,
				newSelect("a", "", nil, newAJ("precedes", false, newSelect("b", "", nil, nil), nil)),
				nil,
				newPagingClause("limit", 10, "offset", 10),
			),
		},
		{
			"limit/offset reversed",
			"from my-robot a precedes b offset 10 limit 10",
			newQuery(
				nil,
				newSelect("a", "", nil, newAJ("precedes", false, newSelect("b", "", nil, nil), nil)),
				nil,
				newPagingClause("offset", 10, "limit", 10),
			),
		},
		{
			"newlines elided",
			`from my-robot a, b
			where a.foo = 10 and b.bar = 20
			limit 10 offset 10`,
			newQuery(
				nil,
				newSelect("a", "", newMJ(newSelect("b", "", nil, nil)), nil),
				newWhere(
					newOr(
						newBinaryExpr("a.foo", "=", int64(10)),
						newBinaryExpr("b.bar", "=", int64(20)),
					),
				),
				newPagingClause("limit", 10, "offset", 10),
			),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, *ast)
		})
	}
}

/*
The remainder of the file is functions used to create AST nodes for testing.

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
*/

// newPagingClause returns a new paging clause.
func newPagingClause(kvs ...any) []ql.PagingTerm {
	if len(kvs)%2 != 0 {
		panic("invalid number of arguments")
	}
	terms := make([]ql.PagingTerm, 0, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		keyword := kvs[i].(string)
		value := kvs[i+1].(int)
		terms = append(terms, newPagingTerm(keyword, value))
	}
	return terms
}

// newQuery returns a new query.
func newQuery(
	between *ql.Between,
	sel ql.Select,
	where []ql.OrClause,
	paging []ql.PagingTerm,
) ql.Query {
	return ql.Query{
		From:         producer,
		Between:      between,
		Select:       sel,
		Where:        where,
		PagingClause: paging,
	}
}

// newBetween returns a new between clause.
func newBetween(a, b any) *ql.Between {
	var from, to ql.Timestamp
	switch a := a.(type) {
	case int:
		from = ql.Timestamp{Nanoseconds: util.Pointer(int64(a))}
	case int64:
		from = ql.Timestamp{Nanoseconds: &a}
	case string:
		from = ql.Timestamp{Datestring: &a}
	}
	switch b := b.(type) {
	case int:
		to = ql.Timestamp{Nanoseconds: util.Pointer(int64(b))}
	case int64:
		to = ql.Timestamp{Nanoseconds: &b}
	case string:
		to = ql.Timestamp{Datestring: &b}
	}
	return &ql.Between{
		From: from,
		To:   to,
	}
}

// newWhere returns a new where clause.
func newWhere(clauses ...ql.OrClause) []ql.OrClause {
	return clauses
}

// newOr returns a new or clause.
func newOr(exprs ...ql.BinaryExpression) ql.OrClause {
	return ql.OrClause{
		AndExprs: exprs,
	}
}

// newSelect returns a new select statement.
func newSelect(entity string, alias string, mj *ql.MJ, aj *ql.AJ) ql.Select {
	return ql.Select{
		Entity: entity,
		Alias:  alias,
		MJ:     mj,
		AJ:     aj,
	}
}

// newBinaryExpr returns a new binary expression.
func newBinaryExpr(left string, op string, right any) ql.BinaryExpression {
	value := ql.Value{}
	switch right := right.(type) {
	case string:
		value.Text = &right
	case int64:
		value.Integer = &right
	case float64:
		value.Float = &right
	default:
		panic("unsupported type")
	}
	return ql.BinaryExpression{
		Left:  left,
		Op:    op,
		Right: value,
	}
}

// newMJ returns a new merge join.
func newMJ(sel ql.Select) *ql.MJ {
	return &ql.MJ{
		Select: sel,
	}
}

// newValue returns a new value.
func newValue(x any) *ql.Value {
	switch x := x.(type) {
	case string:
		return &ql.Value{Text: &x}
	case int64:
		return &ql.Value{Integer: &x}
	case float64:
		return &ql.Value{Float: &x}
	default:
		panic("unsupported type")
	}
}

// newAJ returns a new as-of join.
func newAJ(keyword string, immediate bool, sel ql.Select, constraint *ql.AsOfConstraint) *ql.AJ {
	return &ql.AJ{
		Keyword:    keyword,
		Immediate:  immediate,
		Select:     sel,
		Constraint: constraint,
	}
}

// newAsofConstraint returns a new as-of constraint.
func newAsofConstraint(quantity int, units string) *ql.AsOfConstraint {
	return &ql.AsOfConstraint{
		Quantity: quantity,
		Units:    units,
	}
}

// newPagingTerm returns a new paging term.
func newPagingTerm(keyword string, value int) ql.PagingTerm {
	return ql.PagingTerm{
		Keyword: keyword,
		Value:   value,
	}
}
