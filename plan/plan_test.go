package plan_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/plan"
	"github.com/wkalt/dp3/ql"
	"github.com/wkalt/dp3/util/testutils"
)

// todo: it would be useful if we could distinguish between the cases where
// multiple tables are referenced from the wrong part of the query, and when
// aliases that are just invalid/inconsistent occur. The current tree traversal
// isn't aware of other extant aliases (or tables) when it does its validation,
// so this will require a bit more effort.
func TestInvalidPlans(t *testing.T) {
	cases := []struct {
		assertion string
		query     string
		output    string
	}{
		{
			"and expression spanning tables",
			"from device a, b where a.foo = 10 and b.bar = 20;",
			"more than one alias",
		},
		{
			"grouped or expression spanning tables",
			"from device a, b where (a.foo = 10 or b.bar = 20);",
			"more than one alias",
		},
		{
			"multiple aliases to same table in one scan",
			"from device a as b where a.foo = 10 and b.bar = 20;",
			"more than one alias",
		},
		{
			"where clauses must be qualified",
			"from device a where foo = 10;",
			"field foo must be qualified with a dot",
		},
		{
			"partly unqualified where clause",
			"from device a where a.foo = 10 and bar = 20;",
			"field bar must be qualified with a dot",
		},
		{
			"where clause qualified with alias that doesn't exist",
			"from device a where b.foo = 10;",
			"unresolved table alias: b",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			parser := ql.NewParser()
			ast, err := parser.ParseString("", c.query)
			require.NoError(t, err)
			_, err = plan.CompileQuery("db", *ast)
			require.Error(t, err)
			require.Contains(t, err.Error(), c.output)
		})
	}
}

func TestCompileQuery(t *testing.T) {
	cases := []struct {
		assertion string
		query     string
		output    string
	}{
		{
			"single scan",
			"from device a;",
			"[scan (a db device all-time)]",
		},
		{
			"descending scan",
			"from device a desc;",
			"[scan desc (a db device all-time)]",
		},
		{
			"single scan with a where clause",
			"from device a where a.foo = 10;",
			"[scan (a db device all-time) [binexp [= a.foo 10]]]",
		},
		{
			"single scan with multiple where clauses",
			"from device a where a.foo = 10 and a.bar = 20;",
			"[scan (a db device all-time) [and [binexp [= a.foo 10]] [binexp [= a.bar 20]]]]",
		},
		{
			"single scan with or condition",
			"from device a where a.foo = 10 or a.bar = 20;",
			"[scan (a db device all-time) [or [binexp [= a.foo 10]] [binexp [= a.bar 20]]]]",
		},
		{
			"single scan with an alias",
			"from device a as b;",
			"[scan (a b db device all-time)]",
		},
		{
			"aliased where clauses are resolved",
			"from device a as b where b.foo = 1;",
			"[scan (a b db device all-time) [binexp [= b.foo 1]]]",
		},
		{
			"multiple aliased where clauses are resolved",
			"from device a as b, c as d where b.foo = 1 or d.bar = 2;",
			`[merge
			   [scan (a b db device all-time) [binexp [= b.foo 1]]]
			   [scan (c d db device all-time) [binexp [= d.bar 2]]]]`,
		},
		{
			"basic mj",
			"from device a, b;",
			`[merge
			  [scan (a db device all-time)]
			  [scan (b db device all-time)]]`,
		},
		{
			"ternary mj",
			"from device a, b, c;",
			`[merge
			  [scan (a db device all-time)]
			  [scan (b db device all-time)]
			  [scan (c db device all-time)]]`,
		},
		{
			"scan with where clause and limit",
			"from device a where a.b = 1 limit 10;",
			`[limit 10
			  [scan (a db device all-time) [binexp [= a.b 1]]]]`,
		},
		{
			"scan with limit and offset",
			"from device a limit 10 offset 5;",
			`[limit 10
			  [offset 5
			    [scan (a db device all-time)]]]`,
		},
		{
			"merge join with descending",
			"from device a, b desc;",
			`[merge desc 
			  [scan desc (a db device all-time)]
			  [scan desc (b db device all-time)]]`,
		},
		{
			"merge join with where clause",
			"from device a, b where a.b = 10 or b.c = 20;",
			`[merge
			  [scan (a db device all-time) [binexp [= a.b 10]]]
			  [scan (b db device all-time) [binexp [= b.c 20]]]]`,
		},
		{
			"asof join with where clause",
			"from device a precedes b where b.c = 10 or a.b = 20;",
			`[asof (precedes full)
			   [scan (a db device all-time) [binexp [= a.b 20]]]
			   [scan (b db device all-time) [binexp [= b.c 10]]]]`,
		},
		{
			"asof join with restriction",
			"from device a precedes b by less than 5 seconds;",
			`[asof (precedes full seconds 5)
			  [scan (a db device all-time)]
			  [scan (b db device all-time)]]`,
		},
		{
			"asof join with aliasing",
			"from device a as foo precedes b as bar by less than 5 seconds;",
			`[asof (precedes full seconds 5)
			  [scan (a foo db device all-time)]
			  [scan (b bar db device all-time)]]`,
		},
		{
			"trivial subexpressions are pulled up",
			"from devices a where (a.foo = 10);",
			`[scan (a db devices all-time)
			  [binexp [= a.foo 10]]]`,
		},
		{
			"grouped subexpressions on a single scan",
			"from devices a where (a.foo = 10 or a.bar = 20) and a.baz = 30;",
			`[scan (a db devices all-time)
			  [and
			    [or [binexp [= a.foo 10]] [binexp [= a.bar 20]]]
			    [binexp [= a.baz 30]]]]`,
		},
		{
			"grouped subexpressions on multiple scans",
			"from devices a, b where (a.foo = 10 or a.bar = 20) or b.baz = 30;",
			`[merge
			  [scan (a db devices all-time) [or [binexp [= a.foo 10]] [binexp [= a.bar 20]]]]
			  [scan (b db devices all-time) [binexp [= b.baz 30]]]]`,
		},
		{
			"merge join with aliases",
			"from device a as b, c as d;",
			`[merge
			  [scan (a b db device all-time)]
			  [scan (c d db device all-time)]]`,
		},
	}
	parser := ql.NewParser()
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.query)
			require.NoError(t, err)
			plan, err := plan.CompileQuery("db", *ast)
			require.NoError(t, err)
			require.Equal(t, testutils.StripSpace(c.output), plan.String())
		})
	}
}

func TestQueryCompilationErrors(t *testing.T) {
	cases := []struct {
		assertion string
		query     string
		output    string
	}{
		{
			"invalid alias reference",
			"from device /fix as f where b.foo = 10;",
			"unresolved table alias: b",
		},
		{
			"missing alias",
			"from device /fix where b.foo = 10;",
			"unresolved table alias: b",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			parser := ql.NewParser()
			ast, err := parser.ParseString("", c.query)
			require.NoError(t, err)
			_, err = plan.CompileQuery("db", *ast)
			require.ErrorIs(t, err, plan.BadPlanError{})
			require.Contains(t, err.Error(), c.output)
		})
	}
}
