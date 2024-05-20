package executor_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/executor"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/plan"
	"github.com/wkalt/dp3/ql"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/schema"
)

type statconfig struct {
	field    string
	fieldmin any
	fieldmax any
}

func newChild(t *testing.T, configs ...statconfig) *nodestore.Child {
	t.Helper()
	child := &nodestore.Child{}
	statistics := nodestore.NewStatistics([]util.Named[schema.PrimitiveType]{
		util.NewNamed("int8", schema.INT8),         // 0
		util.NewNamed("int16", schema.INT16),       // 1
		util.NewNamed("int32", schema.INT32),       // 2
		util.NewNamed("int64", schema.INT64),       // 3
		util.NewNamed("uint8", schema.UINT8),       // 4
		util.NewNamed("uint16", schema.UINT16),     // 5
		util.NewNamed("uint32", schema.UINT32),     // 6
		util.NewNamed("uint64", schema.UINT64),     // 7
		util.NewNamed("float32", schema.FLOAT32),   // 8
		util.NewNamed("float64", schema.FLOAT64),   // 9
		util.NewNamed("string", schema.STRING),     // 10
		util.NewNamed("bool", schema.BOOL),         // 11
		util.NewNamed("time", schema.TIME),         // 12
		util.NewNamed("duration", schema.DURATION), // 13
	})

	for _, config := range configs {
		field := config.field
		fieldmin := config.fieldmin
		fieldmax := config.fieldmax
		idx := slices.IndexFunc(statistics.Fields,
			func(f util.Named[schema.PrimitiveType]) bool {
				return f.Name == field
			})
		switch fieldmin := fieldmin.(type) {
		case int:
			statistics.NumStats[idx] = &nodestore.NumericalSummary{
				Min: float64(fieldmin),
				Max: float64(fieldmax.(int)),
			}
		case float64:
			statistics.NumStats[idx] = &nodestore.NumericalSummary{
				Min: fieldmin,
				Max: fieldmax.(float64),
			}
		case string:
			statistics.TextStats[idx] = &nodestore.TextSummary{
				Min: fieldmin,
				Max: fieldmax.(string),
			}
		default:
			t.Error("invalid type")
		}
	}

	child.Statistics = map[string]*nodestore.Statistics{
		"": statistics,
	}
	return child
}

func extractWhere(t *testing.T, query string) *plan.Node {
	t.Helper()
	parser := ql.NewParser()
	ast, err := parser.ParseString("", query)
	require.NoError(t, err)
	qp, err := plan.CompileQuery("db", *ast)
	require.NoError(t, err)

	var where *plan.Node
	require.NoError(t, plan.Traverse(qp, func(n *plan.Node) error {
		if n.Type == plan.Scan && len(n.Children) > 0 {
			where = n.Children[0]
		}
		return nil
	}, nil))

	return where
}

func TestExpressionStatFilters(t *testing.T) {
	cases := []struct {
		assertion string
		query     string
		inputs    [][]statconfig
		expected  []bool
	}{
		{
			"no restriction",
			"from my-robot /topic;",
			[][]statconfig{
				{{"int8", 0, 5}}, // one child
			},
			[]bool{true},
		},
		{
			"basic filter",
			"from my-robot /topic where /topic.int8 > 5;",
			[][]statconfig{
				{{"int8", 0, 5}},
				{{"int8", 5, 10}},
			},
			[]bool{false, true},
		},
		{
			"or condition on one column",
			"from my-robot /topic where /topic.int8 > 5 or /topic.int8 < 2;",
			[][]statconfig{
				{{"int8", 0, 5}},
				{{"int8", 2, 5}},
			},
			[]bool{true, false},
		},
		{
			"or condition on two columns",
			"from my-robot /topic where /topic.int8 > 5 or /topic.int16 < 2;",
			[][]statconfig{
				{{"int8", 0, 5}, {"int16", 0, 5}},
				{{"int8", 5, 10}, {"int16", 10, 20}},
			},
			[]bool{true, true},
		},
		{
			"and condition on two columns",
			"from my-robot /topic where /topic.int8 > 5 and /topic.int16 < 2;",
			[][]statconfig{
				{{"int8", 0, 5}, {"int16", 0, 5}},
				{{"int8", 5, 10}, {"int16", 0, 2}},
			},
			[]bool{false, true},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			where := extractWhere(t, c.query)
			filter, err := executor.NewStatFilter(where)
			require.NoError(t, err)

			for i, inputs := range c.inputs {
				child := newChild(t, inputs...)
				actual, err := filter(child)
				require.NoError(t, err)
				if actual != c.expected[i] {
					t.Errorf("expected %v, got %v at index %d", c.expected[i], actual, i)
				}
			}
		})
	}
}

func TestStringStatFilters(t *testing.T) {
	stringTypes := []string{
		"string",
	}

	cases := []struct {
		operator string
		expected []bool
	}{
		{
			"<",
			[]bool{true, false, false},
		},
		{
			"<=",
			[]bool{true, true, false},
		},
		{
			">",
			[]bool{false, true, true},
		},
		{
			">=",
			[]bool{true, true, true},
		},
		{
			"=",
			[]bool{true, true, false},
		},
	}

	for _, name := range stringTypes {
		children := []*nodestore.Child{
			newChild(t, statconfig{name, "a", "e"}),
			newChild(t, statconfig{name, "e", "i"}),
			newChild(t, statconfig{name, "i", "o"}),
		}

		for _, c := range cases {
			t.Run(name+" "+c.operator, func(t *testing.T) {
				query := basicScan(name, c.operator, "\"e\"")
				node := extractWhere(t, query)
				filter, err := executor.NewStatFilter(node)
				require.NoError(t, err)

				for i, child := range children {
					actual, err := filter(child)
					require.NoError(t, err)
					if actual != c.expected[i] {
						t.Errorf("expected %v, got %v at index %d", c.expected[i], actual, i)
					}
				}
			})
		}
	}
}

func basicScan(field string, operator string, term string) string {
	return "from my-robot /topic where /topic." + field + " " + operator + " " + term + ";"
}

func TestFloatStatFilters(t *testing.T) {
	floatTypes := []string{
		"float32", "float64",
	}

	cases := []struct {
		operator string
		expected []bool
	}{
		{
			"<",
			[]bool{true, false, false},
		},
		{
			"<=",
			[]bool{true, true, false},
		},
		{
			">",
			[]bool{false, true, true},
		},
		{
			">=",
			[]bool{true, true, true},
		},
		{
			"=",
			[]bool{true, true, false},
		},
	}

	for _, name := range floatTypes {
		children := []*nodestore.Child{
			newChild(t, statconfig{name, 0.0, 5.0}),
			newChild(t, statconfig{name, 5.0, 10.0}),
			newChild(t, statconfig{name, 10.0, 20.0}),
		}

		for _, c := range cases {
			t.Run(name+" "+c.operator, func(t *testing.T) {
				query := basicScan(name, c.operator, "5.0")
				node := extractWhere(t, query)
				filter, err := executor.NewStatFilter(node)
				require.NoError(t, err)

				for i, child := range children {
					actual, err := filter(child)
					require.NoError(t, err)
					if actual != c.expected[i] {
						t.Errorf("expected %v, got %v at index %d", c.expected[i], actual, i)
					}
				}
			})
		}
	}
}

func TestIntegerStatFilters(t *testing.T) {
	integerTypes := []string{
		"int8", "int16", "int32", "int64",
		"uint8", "uint16", "uint32", "uint64",
	}

	cases := []struct {
		operator string
		expected []bool
	}{
		{
			"<",
			[]bool{true, false, false},
		},
		{
			"<=",
			[]bool{true, true, false},
		},
		{
			">",
			[]bool{false, true, true},
		},
		{
			">=",
			[]bool{true, true, true},
		},
		{
			"=",
			[]bool{true, true, false},
		},
	}

	for _, name := range integerTypes {
		children := []*nodestore.Child{
			newChild(t, statconfig{name, 0, 5}),
			newChild(t, statconfig{name, 5, 10}),
			newChild(t, statconfig{name, 10, 20}),
		}

		for _, c := range cases {
			t.Run(name+" "+c.operator, func(t *testing.T) {
				query := basicScan(name, c.operator, "5")
				node := extractWhere(t, query)
				filter, err := executor.NewStatFilter(node)
				require.NoError(t, err)

				for i, child := range children {
					actual, err := filter(child)
					require.NoError(t, err)
					if actual != c.expected[i] {
						t.Errorf("expected %v, got %v at index %d", c.expected[i], actual, i)
					}
				}
			})
		}
	}
}
