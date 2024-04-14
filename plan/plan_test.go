package plan_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/plan"
	"github.com/wkalt/dp3/ql"
	"github.com/wkalt/dp3/util/testutils"
)

func TestCompileQuery(t *testing.T) {
	cases := []struct {
		assertion string
		query     string
		output    string
	}{
		{
			"single scan",
			"from device a",
			"[scan (a device all-time)]",
		},
		{
			"single scan with a where clause",
			"from device a where a.foo = 10",
			"[scan (a device all-time) [or [and [binexp [= a.foo 10]]]]]",
		},
		{
			"single scan with multiple where clauses",
			"from device a where a.foo = 10 and a.bar = 20",
			"[scan (a device all-time) [or [and [binexp [= a.foo 10]] [binexp [= a.bar 20]]]]]",
		},
		{
			"single scan with multiple where clauses",
			"from device a where a.foo = 10 or a.bar = 20",
			"[scan (a device all-time) [or [and [binexp [= a.foo 10]]] [and [binexp [= a.bar 20]]]]]",
		},
		{
			"single scan with an alias",
			"from device a as b",
			"[scan (a b device all-time)]",
		},
		{
			"aliased where clauses are resolved",
			"from device a as b where b.foo = 1",
			"[scan (a b device all-time) [or [and [binexp [= b.foo 1]]]]]",
		},
		{
			"multiple aliased where clauses are resolved",
			"from device a as b, c as d where b.foo = 1 and d.bar = 2",
			`[merge
			   [scan (a b device all-time) [or [and [binexp [= b.foo 1]] [binexp [= d.bar 2]]]]]
			   [scan (c d device all-time) [or [and [binexp [= b.foo 1]] [binexp [= d.bar 2]]]]]]`,
		},
		{
			"basic mj",
			"from device a, b",
			"[merge [scan (a device all-time)] [scan (b device all-time)]]",
		},
		{
			"ternary mj", // todo: pull the merges up
			"from device a, b, c",
			"[merge [scan (a device all-time)] [scan (b device all-time)] [scan (c device all-time)]]",
		},
		{
			"scan with where clause",
			"from device a where a.b = 1",
			"[scan (a device all-time) [or [and [binexp [= a.b 1]]]]]",
		},
		{
			"scan with where clause and limit",
			"from device a where a.b = 1 limit 10",
			"[limit 10 [scan (a device all-time) [or [and [binexp [= a.b 1]]]]]]",
		},
		{
			"merge join with where clause",
			"from device a, b where a.b = 10 and b.c = 20",
			`[merge
			  [scan (a device all-time) [or [and [binexp [= a.b 10]] [binexp [= b.c 20]]]]]
			  [scan (b device all-time) [or [and [binexp [= a.b 10]] [binexp [= b.c 20]]]]]]`,
		},
		{
			"asof join with where clause",
			"from device a precedes b where b.c = 10 and a.b = 20",
			`[asof (precedes full)
			   [scan (a device all-time) [or [and [binexp [= b.c 10]] [binexp [= a.b 20]]]]]
			   [scan (b device all-time) [or [and [binexp [= b.c 10]] [binexp [= a.b 20]]]]]]`,
		},
		{
			"asof join with restriction",
			"from device a precedes b by less than 5 seconds",
			"[asof (precedes full seconds 5) [scan (a device all-time)] [scan (b device all-time)]]",
		},
		{
			"asof join with aliasing",
			"from device a as foo precedes b as bar by less than 5 seconds",
			"[asof (precedes full seconds 5) [scan (a foo device all-time)] [scan (b bar device all-time)]]",
		},
	}
	parser := ql.NewParser()
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.query)
			require.NoError(t, err)
			plan, err := plan.CompileQuery(*ast)
			require.NoError(t, err)
			require.Equal(t, testutils.StripSpace(c.output), plan.String())
		})
	}
}
