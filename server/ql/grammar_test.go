package ql_test

import (
	"testing"

	"github.com/alecthomas/participle/v2"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/ql"
	"github.com/wkalt/dp3/server/util"
)

const (
	producer = "my-robot"
)

func TestExpression(t *testing.T) {
	parser, err := participle.Build[ql.Expression](ql.Options...)
	require.NoError(t, err)

	cases := []struct {
		assertion string
		input     string
		expected  *ql.Expression
	}{
		{
			"simple",
			"a = 10",
			newExpression(
				newOrCondition(
					newCondition(
						newTerm(util.Pointer("a"), nil),
						newConditionRHS("=", *newValue(int64(10))),
					),
				),
			),
		},
		{
			"multiple or",
			"a = 10 or b > 20",
			newExpression(
				newOrCondition(
					newCondition(
						newTerm(util.Pointer("a"), nil),
						newConditionRHS("=", *newValue(int64(10))),
					),
				),
				newOrCondition(
					newCondition(
						newTerm(util.Pointer("b"), nil),
						newConditionRHS(">", *newValue(int64(20))),
					),
				),
			),
		},
		{
			"subexpression",
			"(a = 10)",
			newExpression(
				newOrCondition(
					newCondition(
						newTerm(
							nil,
							newExpression(
								newOrCondition(
									newCondition(
										newTerm(util.Pointer("a"), nil),
										newConditionRHS("=", *newValue(int64(10))),
									),
								),
							),
						),
						nil,
					),
				),
			),
		},
		{
			"multiple and",
			"a = 10 and b = 20",
			newExpression(
				newOrCondition(
					newCondition(
						newTerm(util.Pointer("a"), nil),
						newConditionRHS("=", *newValue(int64(10))),
					),
					newCondition(
						newTerm(util.Pointer("b"), nil),
						newConditionRHS("=", *newValue(int64(20))),
					),
				),
			),
		},
		{
			"and takes precedence over or",
			"a = 10 or b = 20 and c = 30",
			newExpression(
				newOrCondition(
					newCondition(
						newTerm(util.Pointer("a"), nil),
						newConditionRHS("=", *newValue(int64(10))),
					),
				),
				newOrCondition(
					newCondition(
						newTerm(util.Pointer("b"), nil),
						newConditionRHS("=", *newValue(int64(20))),
					),
					newCondition(
						newTerm(util.Pointer("c"), nil),
						newConditionRHS("=", *newValue(int64(30))),
					),
				),
			),
		},
		{
			"and takes precedence over or - reversed",
			"a = 10 and b = 20 or c = 30",
			newExpression(
				newOrCondition(
					newCondition(
						newTerm(util.Pointer("a"), nil),
						newConditionRHS("=", *newValue(int64(10))),
					),
					newCondition(
						newTerm(util.Pointer("b"), nil),
						newConditionRHS("=", *newValue(int64(20))),
					),
				),
				newOrCondition(
					newCondition(
						newTerm(util.Pointer("c"), nil),
						newConditionRHS("=", *newValue(int64(30))),
					),
				),
			),
		},
		{
			"grouping overrides precedence",
			"a = 10 and (b = 20 or c = 30)",
			newExpression(
				newOrCondition(
					newCondition(
						newTerm(util.Pointer("a"), nil),
						newConditionRHS("=", *newValue(int64(10))),
					),
					newCondition(
						newTerm(
							nil,
							newExpression(
								newOrCondition(
									newCondition(
										newTerm(util.Pointer("b"), nil),
										newConditionRHS("=", *newValue(int64(20))),
									),
								),
								newOrCondition(
									newCondition(
										newTerm(util.Pointer("c"), nil),
										newConditionRHS("=", *newValue(int64(30))),
									),
								),
							),
						),
						nil,
					),
				),
			),
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
			"'a'",
			newValue("a"),
		},
		{
			"string starting with a dot",
			"'.a'",
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
			"between 'a' and 'b'",
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

func TestStatement(t *testing.T) {
	parser := participle.MustBuild[ql.Statement](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  ql.Statement
	}{
		{
			"explain",
			"explain from my-robot a;",
			ql.Statement{
				Query: util.Pointer(newQuery(true, nil, newSelect("a", "", nil, nil), nil, false, nil)),
			},
		},
		{
			"read query",
			"from my-robot a;",
			ql.Statement{
				Query: util.Pointer(newQuery(false, nil, newSelect("a", "", nil, nil), nil, false, nil)),
			},
		},
		{
			"truncate",
			"truncate my-robot /topic now;",
			ql.Statement{
				Truncate: &ql.Truncate{
					Producer: "my-robot",
					Topic:    "/topic",
					Now:      true,
				},
			},
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

func TestTruncate(t *testing.T) {
	parser := participle.MustBuild[ql.Truncate](ql.Options...)
	cases := []struct {
		assertion string
		input     string
		expected  *ql.Truncate
	}{
		{
			"truncate now",
			"truncate foo /topic now",
			&ql.Truncate{
				Producer: "foo",
				Topic:    "/topic",
				Now:      true,
			},
		},
		{
			"truncate at time",
			"truncate foo /topic 10",
			&ql.Truncate{
				Producer: "foo",
				Topic:    "/topic",
				Now:      false,
				Time:     ql.Timestamp{Nanoseconds: util.Pointer(int64(10))},
			},
		},
		{
			"truncate at iso date",
			"truncate foo /topic '2021-01-01'",
			&ql.Truncate{
				Producer: "foo",
				Topic:    "/topic",
				Now:      false,
				Time:     ql.Timestamp{Datestring: util.Pointer("2021-01-01")},
			},
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
			"explain from my-robot a",
			newQuery(true, nil, newSelect("a", "", nil, nil), nil, false, nil),
		},
		{
			"simple",
			"from my-robot a",
			newQuery(false, nil, newSelect("a", "", nil, nil), nil, false, nil),
		},
		{
			"with between",
			`from my-robot between 'a' and 'b' a`,
			newQuery(false, newBetween("a", "b"), newSelect("a", "", nil, nil), nil, false, nil),
		},
		{
			"with descending",
			"from my-robot a desc",
			newQuery(false, nil, newSelect("a", "", nil, nil), nil, true, nil),
		},
		{
			"with where",
			"from my-robot a where a = 10",
			newQuery(
				false,
				nil,
				newSelect("a", "", nil, nil),
				newExpression(
					newOrCondition(
						newCondition(newTerm(util.Pointer("a"), nil), newConditionRHS("=", *newValue(int64(10)))))),
				false,
				nil,
			),
		},
		{
			"with where and paging",
			"from my-robot a where a = 10 limit 10 offset 10",
			newQuery(
				false,
				nil,
				newSelect("a", "", nil, nil),
				newExpression(
					newOrCondition(
						newCondition(newTerm(util.Pointer("a"), nil),
							newConditionRHS("=", *newValue(int64(10)))))),
				false,
				newPagingClause("limit", 10, "offset", 10),
			),
		},
		{
			"with where and descending",
			"from my-robot a where a = 10 desc",
			newQuery(
				false,
				nil,
				newSelect("a", "", nil, nil),
				newExpression(
					newOrCondition(
						newCondition(newTerm(util.Pointer("a"), nil),
							newConditionRHS("=", *newValue(int64(10)))))),
				true,
				nil,
			),
		},
		{
			"with where, descending, and paging",
			"from my-robot a where a = 10 desc limit 10 offset 10",
			newQuery(
				false,
				nil,
				newSelect("a", "", nil, nil),
				newExpression(
					newOrCondition(
						newCondition(newTerm(util.Pointer("a"), nil),
							newConditionRHS("=", *newValue(int64(10)))))),
				true,
				newPagingClause("limit", 10, "offset", 10),
			),
		},
		{
			"with multiple where",
			"from my-robot a where a = 10 and b = 20",
			newQuery(
				false,
				nil,
				newSelect("a", "", nil, nil),
				newExpression(newOrCondition(
					newCondition(newTerm(util.Pointer("a"), nil), newConditionRHS("=", *newValue(int64(10)))),
					newCondition(newTerm(util.Pointer("b"), nil), newConditionRHS("=", *newValue(int64(20)))))),
				false,
				nil,
			),
		},
		{
			"merge join with where clauses",
			"from my-robot a, b where a = 10 and b = 20",
			newQuery(
				false,
				nil,
				newSelect("a", "", newMJ(newSelect("b", "", nil, nil)), nil),
				newExpression(newOrCondition(
					newCondition(newTerm(util.Pointer("a"), nil), newConditionRHS("=", *newValue(int64(10)))),
					newCondition(newTerm(util.Pointer("b"), nil), newConditionRHS("=", *newValue(int64(20)))),
				)),
				false,
				nil,
			),
		},
		{
			"basic as-of join",
			"from my-robot a precedes b by less than 10 seconds",
			newQuery(
				false,
				nil,
				newSelect("a", "", nil, newAJ("precedes", false,
					newSelect("b", "", nil, nil),
					newAsofConstraint(10, "seconds"))),
				nil,
				false,
				nil,
			),
		},
		{
			"as-of join without constraint",
			"from my-robot a precedes b",
			newQuery(
				false,
				nil,
				newSelect("a", "", nil, newAJ("precedes", false, newSelect("b", "", nil, nil), nil)),
				nil,
				false,
				nil,
			),
		},
		{
			"as-of join without constraint with limit and offset",
			"from my-robot a precedes b limit 10 offset 10",
			newQuery(
				false,
				nil,
				newSelect("a", "", nil, newAJ("precedes", false, newSelect("b", "", nil, nil), nil)),
				nil,
				false,
				newPagingClause("limit", 10, "offset", 10),
			),
		},
		{
			"limit/offset reversed",
			"from my-robot a precedes b offset 10 limit 10",
			newQuery(
				false,
				nil,
				newSelect("a", "", nil, newAJ("precedes", false, newSelect("b", "", nil, nil), nil)),
				nil,
				false,
				newPagingClause("offset", 10, "limit", 10),
			),
		},
		{
			"newlines elided",
			`from my-robot a, b
			where a.foo = 10 and b.bar = 20
			limit 10 offset 10`,
			newQuery(
				false,
				nil,
				newSelect("a", "", newMJ(newSelect("b", "", nil, nil)), nil),
				newExpression(newOrCondition(
					newCondition(newTerm(util.Pointer("a.foo"), nil), newConditionRHS("=", *newValue(int64(10)))),
					newCondition(newTerm(util.Pointer("b.bar"), nil), newConditionRHS("=", *newValue(int64(20)))),
				)),
				false,
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
		key, ok := kvs[i].(string)
		if !ok {
			panic("invalid key")
		}
		value, ok := kvs[i+1].(int)
		if !ok {
			panic("invalid value")
		}
		terms = append(terms, newPagingTerm(key, value))
	}
	return terms
}

// newQuery returns a new query.
func newQuery(
	explain bool,
	between *ql.Between,
	sel ql.Select,
	where *ql.Expression,
	descending bool,
	paging []ql.PagingTerm,
) ql.Query {
	return ql.Query{
		Explain:      explain,
		From:         producer,
		Between:      between,
		Select:       sel,
		Where:        where,
		Descending:   descending,
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

// newSelect returns a new select statement.
func newSelect(entity string, alias string, mj *ql.MJ, aj *ql.AJ) ql.Select {
	return ql.Select{
		Entity: entity,
		Alias:  alias,
		MJ:     mj,
		AJ:     aj,
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

// newExpression returns a new expression.
func newExpression(or ...*ql.OrCondition) *ql.Expression {
	return &ql.Expression{
		Or: or,
	}
}

// newOrCondition returns a new or condition.
func newOrCondition(and ...*ql.Condition) *ql.OrCondition {
	return &ql.OrCondition{
		And: and,
	}
}

// newCondition returns a new condition.
func newCondition(term ql.Term, rhs *ql.ConditionRHS) *ql.Condition {
	return &ql.Condition{
		Operand: term,
		RHS:     rhs,
	}
}

// newConditionRHS returns a new condition RHS.
func newConditionRHS(op string, value ql.Value) *ql.ConditionRHS {
	return &ql.ConditionRHS{
		Op:    op,
		Value: value,
	}
}

// newTerm returns a new term.
func newTerm(v *string, subexpr *ql.Expression) ql.Term {
	return ql.Term{
		Value:         v,
		Subexpression: subexpr,
	}
}
