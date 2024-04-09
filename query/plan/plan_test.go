package plan_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/query/plan"
	"github.com/wkalt/dp3/query/ql"
)

func TestCompileQuery(t *testing.T) {
	cases := []struct {
		assertion string
		query     string
		output    string
	}{
		//{
		//	"single scan",
		//	"from device a",
		//	"[scan (a device all-time)]",
		//},
		//{
		//	"basic mj",
		//	"from device a, b",
		//	"[merge [scan (a device all-time)] [scan (b device all-time)]]",
		//},
		//{
		//	"ternary mj", // todo: pull the merges up
		//	"from device a, b, c",
		//	"[merge [scan (a device all-time)] [merge [scan (b device all-time)] [scan (c device all-time)]]]",
		//},
		//{
		//	"scan with where clause",
		//	"from device a where a.b = 1",
		//	"[scan (a device all-time) [binexp [= a.b 1]]]",
		//},
		//{
		//	"scan with where clause and limit",
		//	"from device a where a.b = 1 limit 10",
		//	"[limit 10 [scan (a device all-time) [binexp [= a.b 1]]]]",
		//},
		//{
		//	"merge join with where clause",
		//	"from device a, b where a.b = 10 and b.c = 20",
		//	"[merge [scan (a device all-time) [binexp [= a.b 10]]] [scan (b device all-time) [binexp [= b.c 20]]]]",
		//},
		//{
		//	"asof join with where clause",
		//	"from device a precedes b where b.c = 10 and a.b = 20",
		//	"[asof (precedes full) [scan (a device all-time) [binexp [= a.b 20]]] [scan (b device all-time) [binexp [= b.c 10]]]]",
		//},
		{
			"asof join with restriction",
			"from device a precedes b by less than 5 seconds",
			"[asof (precedes full seconds 5) [scan (a device all-time)] [scan (b device all-time)]]",
		},
	}
	parser := ql.NewParser()
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ast, err := parser.ParseString("", c.query)
			require.NoError(t, err)
			plan, err := plan.CompileQuery(*ast)
			require.NoError(t, err)
			require.Equal(t, c.output, plan.String())
		})
	}
}
