package nodestore_test

import (
	"math"
	"testing"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/schema"
	"github.com/wkalt/dp3/util/testutils"
)

func TestObserve(t *testing.T) {
	t.Run("edge case floats do not disrupt stats", func(t *testing.T) {
		cases := []struct {
			assertion string
			value     float64
		}{
			{"NaN", math.NaN()},
			{"+Inf", math.Inf(1)},
			{"-Inf", math.Inf(-1)},
		}
		for _, c := range cases {
			t.Run(c.assertion, func(t *testing.T) {
				s := nodestore.NewStatistics([]util.Named[schema.PrimitiveType]{util.NewNamed("test", schema.FLOAT64)})
				msg := &fmcap.Message{
					Data: testutils.F64b(c.value),
				}
				require.NoError(t, s.ObserveMessage(msg, []any{c.value}))
				require.Equal(t, 1, int(s.MessageCount))
				require.Zero(t, s.NumStats[0].Mean)
				require.Zero(t, s.NumStats[0].Sum)
				require.Zero(t, s.NumStats[0].Min)
				require.Zero(t, s.NumStats[0].Max)
			})
		}
	})

	t.Run("text updates text stats", func(t *testing.T) {
		s := nodestore.NewStatistics([]util.Named[schema.PrimitiveType]{util.NewNamed("test", schema.STRING)})
		data := testutils.PrefixedString("hello")
		msg := &fmcap.Message{
			Data: data,
		}
		require.NoError(t, s.ObserveMessage(msg, []any{"hello"}))
		require.Equal(t, 1, int(s.MessageCount))
		require.Equal(t, "hello", s.TextStats[0].Min)
		require.Equal(t, "hello", s.TextStats[0].Max)
	})

	t.Run("numeric types update numeric stats", func(t *testing.T) {
		cases := []struct {
			name  string
			value any
		}{
			{"int8", int8(1)},
			{"int16", int16(1)},
			{"int32", int32(1)},
			{"int64", int64(1)},
			{"float32", float32(1)},
			{"float64", float64(1)},
			{"uint8", uint8(1)},
			{"uint16", uint16(1)},
			{"uint32", uint32(1)},
			{"uint64", uint64(1)},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				s := nodestore.NewStatistics([]util.Named[schema.PrimitiveType]{util.NewNamed("test", schema.FLOAT64)})
				var data []byte
				switch value := c.value.(type) {
				case int8:
					data = testutils.U8b(uint8(value))
				case uint8:
					data = testutils.U8b(value)
				case int16:
					data = testutils.U16b(uint16(value))
				case uint16:
					data = testutils.U16b(value)
				case int32:
					data = testutils.U32b(uint32(value))
				case uint32:
					data = testutils.U32b(value)
				case int64:
					data = testutils.U64b(uint64(value))
				case uint64:
					data = testutils.U64b(value)
				case float32:
					data = testutils.F32b(value)
				case float64:
					data = testutils.F64b(value)
				}
				msg := &fmcap.Message{
					Data: data,
				}
				require.NoError(t, s.ObserveMessage(msg, []any{c.value}))
				require.Equal(t, 1, int(s.MessageCount))
				require.InEpsilon(t, float64(1), s.NumStats[0].Mean, 0.01)
				require.InEpsilon(t, float64(1), s.NumStats[0].Sum, 0.01)
				require.InEpsilon(t, float64(1), s.NumStats[0].Min, 0.01)
				require.InEpsilon(t, float64(1), s.NumStats[0].Max, 0.01)
			})
		}
	})
}

func TestAdd(t *testing.T) {
	cases := []struct {
		assertion string
		inputs    []*nodestore.Statistics
		output    *nodestore.Statistics
	}{
		{
			assertion: "adding to empty copies the input",
			inputs: []*nodestore.Statistics{
				{},
				{
					MessageCount:      1,
					BytesUncompressed: 10,
					MaxObservedTime:   10,
					MinObservedTime:   1,
				}},
			output: &nodestore.Statistics{
				MessageCount:      1,
				BytesUncompressed: 10,
				MaxObservedTime:   10,
				MinObservedTime:   1,
			},
		},
		{
			"adding to a populated statistic",
			[]*nodestore.Statistics{
				{
					MessageCount:      1,
					BytesUncompressed: 10,
					MaxObservedTime:   10,
					MinObservedTime:   1,
				},
				{
					MessageCount:      1,
					BytesUncompressed: 10,
					MaxObservedTime:   20,
					MinObservedTime:   1,
				},
			},
			&nodestore.Statistics{
				MessageCount:      2,
				BytesUncompressed: 20,
				MaxObservedTime:   20,
				MinObservedTime:   1,
			},
		},
		{
			"adding text fields",
			[]*nodestore.Statistics{
				{
					Fields: []util.Named[schema.PrimitiveType]{util.NewNamed("test", schema.STRING)},
					TextStats: map[int]*nodestore.TextSummary{
						0: {
							Min: "a",
							Max: "b",
						},
					},
					MessageCount: 1,
				},
				{
					Fields: []util.Named[schema.PrimitiveType]{util.NewNamed("test", schema.STRING)},
					TextStats: map[int]*nodestore.TextSummary{
						0: {
							Min: "a",
							Max: "c",
						},
					},
					MessageCount: 1,
				},
			},
			&nodestore.Statistics{
				Fields: []util.Named[schema.PrimitiveType]{util.NewNamed("test", schema.STRING)},
				TextStats: map[int]*nodestore.TextSummary{
					0: {
						Min: "a",
						Max: "c",
					},
				},
				MessageCount: 2,
			},
		},
		{
			"adding numeric fields",
			[]*nodestore.Statistics{
				{
					Fields: []util.Named[schema.PrimitiveType]{util.NewNamed("test", schema.FLOAT64)},
					NumStats: map[int]*nodestore.NumericalSummary{
						0: {
							Min:   1,
							Max:   10,
							Sum:   10,
							Mean:  5,
							Count: 2,
						},
					},
					MessageCount: 2,
				},
				{
					Fields: []util.Named[schema.PrimitiveType]{util.NewNamed("test", schema.FLOAT64)},
					NumStats: map[int]*nodestore.NumericalSummary{
						0: {
							Min:   10,
							Max:   20,
							Sum:   30,
							Mean:  15,
							Count: 2,
						},
					},
					MessageCount: 2,
				},
			},
			&nodestore.Statistics{
				Fields: []util.Named[schema.PrimitiveType]{util.NewNamed("test", schema.FLOAT64)},
				NumStats: map[int]*nodestore.NumericalSummary{
					0: {
						Min:   1,
						Max:   20,
						Mean:  10,
						Sum:   40,
						Count: 4,
					},
				},
				MessageCount: 4,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			first := c.inputs[0]
			second := c.inputs[1]
			require.NoError(t, first.Add(second))
			require.Equal(t, c.output, first)
		})
	}
}
