package executor_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/query/executor"
	"github.com/wkalt/dp3/query/plan"
	"github.com/wkalt/dp3/query/ql"
	"github.com/wkalt/dp3/treemgr"
)

func TestCompilePlan(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		query     string
		expected  string
	}{
		{
			"simple scan",
			"from device topic-0",
			"[scan topic-0]",
		},
		{
			"simple scan with time boundaries",
			"from device between 10 and 100 topic-0",
			"[scan topic-0]",
		},
		{
			"simple scan with limit",
			"from device topic-0 limit 10",
			"[limit 10 [scan topic-0]]",
		},
		{
			"simple scan with offset",
			"from device topic-0 offset 10",
			"[offset 10 [scan topic-0]]",
		},
		{
			"merge join",
			"from device topic-0, topic-1",
			"[merge [scan topic-0] [scan topic-1]]",
		},
		{
			"asof join",
			"from device topic-0 precedes topic-1 by less than 10 seconds",
			"[asof 10000000000 [scan topic-0] [scan topic-1]]",
		},
	}
	parser := ql.NewParser()
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			tmgr, finish := treemgr.TestTreeManager(ctx, t)
			defer finish()
			prepTmgr(t, ctx, tmgr)

			ast, err := parser.ParseString("", c.query)
			require.NoError(t, err)

			qp, err := plan.CompileQuery(*ast)
			require.NoError(t, err)

			actual, err := executor.CompilePlan(ctx, qp, tmgr.NewTreeIterator)
			require.NoError(t, err)
			require.Equal(t, c.expected, actual.String())
		})
	}
}

func prepTmgr(t *testing.T, ctx context.Context, tmgr *treemgr.TreeManager) {
	t.Helper()
	buf := &bytes.Buffer{}
	mcap.WriteFile(t, buf, [][]int64{{1, 3, 5}, {2, 4, 6}}...)
	require.NoError(t, tmgr.Receive(ctx, "device", buf))
	require.NoError(t, tmgr.ForceFlush(ctx))
}
