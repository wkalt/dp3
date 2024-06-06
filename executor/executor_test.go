package executor_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"testing"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/executor"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/plan"
	"github.com/wkalt/dp3/ql"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/testutils"
)

func TestQueryExecution(t *testing.T) {
	ctx := context.Background()
	tmgr, finish := treemgr.TestTreeManager(ctx, t)
	defer finish()
	prepTmgr2(t, ctx, tmgr)

	t.Run("join scenarios", func(t *testing.T) {
		type message util.Pair[string, uint64] // topic, timestamp
		cases := []struct {
			assertion string
			query     string
			expected  []message
		}{
			{
				"basic scan",
				"from device t0;",
				[]message{{"t0", 0}, {"t0", 1}, {"t0", 2}, {"t0", 3}, {"t0", 4}},
			},
			{
				"scan matching no data",
				"from device t0 where t0.u8 = 100;",
				[]message{},
			},
			{
				"basic descending scan",
				"from device t0 desc;",
				[]message{{"t0", 4}, {"t0", 3}, {"t0", 2}, {"t0", 1}, {"t0", 0}},
			},
			{
				"basic merge join",
				"from device t0, t1;",
				[]message{
					{"t0", 0}, {"t1", 0}, {"t0", 1}, {"t0", 2}, {"t1", 2},
					{"t0", 3}, {"t0", 4}, {"t1", 4}, {"t1", 6}, {"t1", 8},
				},
			},
			{
				"merge join with where clause on one element",
				"from device t0, t1 where t1.u8 = 0;",
				[]message{{"t0", 0}, {"t1", 0}, {"t0", 1}, {"t0", 2}, {"t0", 3}, {"t0", 4}},
			},
			{
				"merge join with where clause on both elements",
				"from device t0, t1 where t0.u8 = 0 or t1.u8 = 0;",
				[]message{{"t0", 0}, {"t1", 0}},
			},
			{
				"asof join precedes",
				"from device t0 precedes t1 by less than 2 nanoseconds;",
				[]message{{"t0", 0}, {"t1", 0}, {"t0", 2}, {"t1", 2}, {"t0", 4}, {"t1", 4}},
			},
			{
				"asof join succeeds",
				"from device t0 succeeds t1 by less than 2 nanoseconds;",
				[]message{{"t1", 0}, {"t0", 0}, {"t0", 1}, {"t1", 2}, {"t0", 2}, {"t0", 3}, {"t1", 4}, {"t0", 4}},
			},
			{
				"asof join with precedes without immediate",
				"from device t1 precedes t8 by less than 100 nanoseconds;",
				[]message{{"t1", 0}, {"t8", 0}, {"t1", 8}, {"t8", 9}, {"t8", 18}, {"t8", 27}, {"t8", 36}},
			},
			{
				"asof join with precedes with immediate",
				"from device t1 precedes immediate t8 by less than 100 nanoseconds;",
				[]message{{"t1", 0}, {"t8", 0}, {"t1", 8}, {"t8", 9}},
			},
			{
				"asof join with where clause",
				"from device t0 precedes immediate t1 by less than 10 nanoseconds where t0.u8 = 0;",
				[]message{{"t0", 0}, {"t1", 0}},
			},
			{
				"merge join with alias",
				"from device t0 as a, t1 as b where a.u8 = 0 or b.u8 = 0;",
				[]message{{"t0", 0}, {"t1", 0}},
			},
			{
				"merge join one alias one not",
				"from device t0 as a, t1 where a.u8 = 0 or t1.u8 = 0;",
				[]message{{"t0", 0}, {"t1", 0}},
			},
			{
				"asof join with alias",
				"from device t0 as a precedes t1 as b by less than 10 nanoseconds where a.u8 = 0 or b.u8 = 0;",
				[]message{{"t0", 0}, {"t1", 0}},
			},
			{
				"limit",
				"from device t0 as a precedes t1 as b by less than 10 nanoseconds where a.u8 = 0 or b.u8 = 0 limit 1;",
				[]message{{"t0", 0}},
			},
			{
				"offset",
				"from device t0 as a precedes t1 as b by less than 10 nanoseconds where a.u8 = 0 or b.u8 = 0 offset 1;",
				[]message{{"t1", 0}},
			},
		}
		for _, c := range cases {
			t.Run(c.assertion, func(t *testing.T) {
				parser := ql.NewParser()
				ast, err := parser.ParseString("", c.query)
				require.NoError(t, err)
				qp, err := plan.CompileQuery("db", *ast)
				require.NoError(t, err)

				buf := &bytes.Buffer{}
				require.NoError(t, executor.Run(ctx, buf, qp, tmgr.NewTreeIterator, false, 0, 0, false))

				reader, err := mcap.NewReader(bytes.NewReader(buf.Bytes()))
				require.NoError(t, err)

				it, err := reader.Messages()
				require.NoError(t, err)

				results := []message{}
				for {
					_, channel, msg, err := it.NextInto(nil)
					if err != nil {
						require.ErrorIs(t, err, io.EOF)
						break
					}
					results = append(results, message{channel.Topic, msg.LogTime})
				}
				require.Equal(t, c.expected, results)
			})
		}
	})

	t.Run("string comparisons", func(t *testing.T) {
		queries := map[string][][]int64{
			`= 'hello'`:   {{0, 0}, {0, 1}, {0, 2}, {0, 3}, {0, 4}},
			`< 'hello'`:   {},
			`> 'hello'`:   {},
			`<= 'hello'`:  {{0, 0}, {0, 1}, {0, 2}, {0, 3}, {0, 4}},
			`>= 'hello'`:  {{0, 0}, {0, 1}, {0, 2}, {0, 3}, {0, 4}},
			`!= 'hello'`:  {},
			`~ 'ello'`:    {{0, 0}, {0, 1}, {0, 2}, {0, 3}, {0, 4}},
			`~ '^ello'`:   {},
			`~ '^hello$'`: {{0, 0}, {0, 1}, {0, 2}, {0, 3}, {0, 4}},
			`~* 'HeLLo'`:  {{0, 0}, {0, 1}, {0, 2}, {0, 3}, {0, 4}},
		}
		parser := ql.NewParser()
		for query, result := range queries {
			t.Run(query, func(t *testing.T) {
				query := "from device t0 where t0.s " + query + ";"
				ast, err := parser.ParseString("", query)
				require.NoError(t, err)
				qp, err := plan.CompileQuery("db", *ast)
				require.NoError(t, err)
				actual, err := executor.CompilePlan(ctx, qp, tmgr.NewTreeIterator)
				require.NoError(t, err)

				results := [][]int64{}
				for {
					tuple, err := actual.Next(ctx)
					if err != nil {
						require.ErrorIs(t, err, io.EOF)
						break
					}
					results = append(results, []int64{int64(tuple.ChannelID()), int64(tuple.LogTime())})
				}
				require.Equal(t, result, results)
			})
		}
	})

	t.Run("numeric comparisons", func(t *testing.T) {
		fields := []string{
			"u8",
			"u16",
			"u32",
			"u64",
			"i8",
			"i16",
			"i32",
			"i64",
			"f32",
			"f64",
		}
		operators := []string{
			"=",
			"<",
			">",
			"<=",
			">=",
			"!=",
		}
		parser := ql.NewParser()
		for _, field := range fields {
			for _, operator := range operators {
				t.Run(fmt.Sprintf("%s %s", field, operator), func(t *testing.T) {
					query := fmt.Sprintf("from device t0 where t0.%s %s 1;", field, operator)
					expected := map[string][][]int64{
						"=":  {{0, 1}},
						"<":  {{0, 0}},
						">":  {{0, 2}, {0, 3}, {0, 4}},
						"<=": {{0, 0}, {0, 1}},
						">=": {{0, 1}, {0, 2}, {0, 3}, {0, 4}},
						"!=": {{0, 0}, {0, 2}, {0, 3}, {0, 4}},
					}
					ast, err := parser.ParseString("", query)
					require.NoError(t, err)
					qp, err := plan.CompileQuery("db", *ast)
					require.NoError(t, err)
					actual, err := executor.CompilePlan(ctx, qp, tmgr.NewTreeIterator)
					require.NoError(t, err)

					results := [][]int64{}
					for {
						tuple, err := actual.Next(ctx)
						if err != nil {
							require.ErrorIs(t, err, io.EOF)
							break
						}
						results = append(results, []int64{int64(tuple.ChannelID()), int64(tuple.LogTime())})
					}
					require.Equal(t, expected[operator], results)
				})
			}
		}
	})
}

func TestCompilePlan(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		query     string
		expected  string
	}{
		{
			"simple scan",
			"from device topic-0;",
			"[scan topic-0]",
		},
		{
			"simple scan with where clause",
			"from device topic-0 where topic-0.foo = 10;",
			"[filter [scan topic-0]]",
		},
		{
			"simple scan with time boundaries",
			"from device between 10 and 100 topic-0;",
			"[scan topic-0]",
		},
		{
			"simple scan with limit",
			"from device topic-0 limit 10;",
			"[limit 10 [scan topic-0]]",
		},
		{
			"simple scan with offset",
			"from device topic-0 offset 10;",
			"[offset 10 [scan topic-0]]",
		},
		{
			"merge join",
			"from device topic-0, topic-1;",
			"[merge [scan topic-0] [scan topic-1]]",
		},
		{
			"merge join with qualification on one side",
			"from device topic-0, topic-1 where topic-0.foo = 10;",
			"[merge [filter [scan topic-0]] [scan topic-1]]",
		},
		{
			"asof join",
			"from device topic-0 precedes topic-1 by less than 10 seconds;",
			"[asof 10000000000 full [scan topic-0] [scan topic-1]]",
		},
		{
			"asof join with immediate",
			"from device topic-0 precedes immediate topic-1 by less than 10 seconds;",
			"[asof 10000000000 immediate [scan topic-0] [scan topic-1]]",
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

			qp, err := plan.CompileQuery("db", *ast)
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
	require.NoError(t, tmgr.Receive(ctx, "db", "device", buf))
	require.NoError(t, tmgr.ForceFlush(ctx))
}

func prepTmgr2(t *testing.T, ctx context.Context, tmgr *treemgr.TreeManager) {
	t.Helper()
	schema := []byte(`
	uint8 u8
	uint16 u16
	uint32 u32
	uint64 u64
	int8 i8
	int16 i16
	int32 i32
	int64 i64
	float32 f32
	float64 f64
	string s
	`)

	buf := &bytes.Buffer{}
	for i := 0; i < 10; i++ {
		w, err := mcap.NewWriter(buf)
		require.NoError(t, err)

		require.NoError(t, w.WriteHeader(&fmcap.Header{}))
		require.NoError(t, w.WriteSchema(&fmcap.Schema{
			ID:       uint16(i) + 1,
			Name:     fmt.Sprintf("schema-%d", i),
			Encoding: "ros1msg",
			Data:     schema,
		}))
		require.NoError(t, w.WriteChannel(&fmcap.Channel{
			ID:              uint16(i),
			SchemaID:        uint16(i) + 1,
			Topic:           fmt.Sprintf("t%d", i),
			MessageEncoding: "ros1msg",
		}))
		c := 0
		for c < 5 {
			data := testutils.Flatten(
				testutils.U8b(uint8(c)),
				testutils.U16b(uint16(c)),
				testutils.U32b(uint32(c)),
				testutils.U64b(uint64(c)),
				testutils.I8b(int8(c)),
				testutils.I16b(int16(c)),
				testutils.I32b(int32(c)),
				testutils.I64b(int64(c)),
				testutils.F32b(float32(c)),
				testutils.F64b(float64(c)),
				testutils.PrefixedString("hello"),
			)

			log.Println("topic", "t"+fmt.Sprint(i), "time", c+i*c)

			require.NoError(t, w.WriteMessage(&fmcap.Message{
				ChannelID:   uint16(i),
				Sequence:    uint32(c),
				LogTime:     uint64(c + i*c),
				PublishTime: 0,
				Data:        data,
			}))
			c++
		}
		require.NoError(t, w.Close())
		require.NoError(t, tmgr.Receive(ctx, "db", "device", buf))
		require.NoError(t, tmgr.ForceFlush(ctx))
		buf.Reset()
	}
}
