package treemgr_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/schema"
	"github.com/wkalt/dp3/versionstore"
)

func TestGetStatisticsLatest(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion   string
		input       [][]uint64
		granularity uint64
		topics      []string
		bounds      []uint64
		ranges      map[string][]tree.StatRange
	}{
		{
			"single topic file",
			[][]uint64{{10, 100, 1000}},
			600 * 1e9,
			[]string{"topic-0"},
			[]uint64{0, 1001},
			map[string][]tree.StatRange{
				"topic-0": {
					{
						Start: 0,
						End:   60e9,
						Statistics: &nodestore.Statistics{
							Fields: []util.Named[schema.PrimitiveType]{
								util.NewNamed("data", schema.STRING),
								util.NewNamed("count", schema.INT16),
							},
							TextStats: map[int]*nodestore.TextSummary{
								0: {
									Max: "hello",
									Min: "hello",
								},
							},
							NumStats: map[int]*nodestore.NumericalSummary{
								1: {
									Max:  2024,
									Min:  2024,
									Sum:  6072,
									Mean: 2024,
								},
							},
							MessageCount:    3,
							ByteCount:       33,
							MaxObservedTime: 1000,
							MinObservedTime: 10,
						},
					},
				},
			},
		},
		{
			"multiple buckets",
			[][]uint64{{10, 100, 1000, 90e9}},
			600 * 1e9,
			[]string{"topic-0"},
			[]uint64{0, 100e9},
			map[string][]tree.StatRange{
				"topic-0": {
					{
						Start: 0,
						End:   60e9,
						Statistics: &nodestore.Statistics{
							Fields: []util.Named[schema.PrimitiveType]{
								util.NewNamed("data", schema.STRING),
								util.NewNamed("count", schema.INT16),
							},
							NumStats: map[int]*nodestore.NumericalSummary{
								1: {
									Max:  2024,
									Min:  2024,
									Sum:  6072,
									Mean: 2024,
								},
							},
							TextStats: map[int]*nodestore.TextSummary{
								0: {
									Max: "hello",
									Min: "hello",
								},
							},
							MessageCount:    3,
							ByteCount:       33,
							MaxObservedTime: 1000,
							MinObservedTime: 10,
						},
					},
					{
						Start: 60e9,
						End:   120e9,
						Statistics: &nodestore.Statistics{
							Fields: []util.Named[schema.PrimitiveType]{
								util.NewNamed("data", schema.STRING),
								util.NewNamed("count", schema.INT16),
							},
							NumStats: map[int]*nodestore.NumericalSummary{
								1: {
									Max:  2024,
									Min:  2024,
									Sum:  2024,
									Mean: 2024,
								},
							},
							TextStats: map[int]*nodestore.TextSummary{
								0: {
									Max: "hello",
									Min: "hello",
								},
							},
							MessageCount:    1,
							ByteCount:       11,
							MaxObservedTime: 90e9,
							MinObservedTime: 90e9,
						},
					},
				},
			},
		},
		{
			"multiple buckets, low granularity",
			[][]uint64{{10, 100, 1000, 90e9}},
			64 * 600 * 1e9,
			[]string{"topic-0"},
			[]uint64{0, 100e9},
			map[string][]tree.StatRange{
				"topic-0": {
					{
						Start: 0,
						End:   3840000000000,
						Statistics: &nodestore.Statistics{
							Fields: []util.Named[schema.PrimitiveType]{
								util.NewNamed("data", schema.STRING),
								util.NewNamed("count", schema.INT16),
							},
							NumStats: map[int]*nodestore.NumericalSummary{
								1: {
									Max:  2024,
									Min:  2024,
									Sum:  8096,
									Mean: 2024,
								},
							},
							TextStats: map[int]*nodestore.TextSummary{
								0: {
									Max: "hello",
									Min: "hello",
								},
							},
							MessageCount:    4,
							ByteCount:       44,
							MaxObservedTime: 90e9,
							MinObservedTime: 10,
						},
					},
				},
			},
		},
		{
			"excludes buckets based on [) semantics",
			[][]uint64{{10, 100, 1000, 90e9}},
			600 * 1e9,
			[]string{"topic-0"},
			[]uint64{100, 60e9},
			map[string][]tree.StatRange{
				"topic-0": {
					{
						Start: 0,
						End:   60e9,
						Statistics: &nodestore.Statistics{
							Fields: []util.Named[schema.PrimitiveType]{
								util.NewNamed("data", schema.STRING),
								util.NewNamed("count", schema.INT16),
							},
							NumStats: map[int]*nodestore.NumericalSummary{
								1: {
									Max:  2024,
									Min:  2024,
									Sum:  6072,
									Mean: 2024,
								},
							},
							TextStats: map[int]*nodestore.TextSummary{
								0: {
									Max: "hello",
									Min: "hello",
								},
							},
							MessageCount:    3,
							ByteCount:       33,
							MaxObservedTime: 1000,
							MinObservedTime: 10,
						},
					},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			mcap.WriteFile(t, buf, c.input...)
			tmgr := testTreeManager(ctx, t)
			require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
			require.NoError(t, tmgr.SyncWAL(ctx))
			start := c.bounds[0]
			end := c.bounds[1]

			result := make(map[string][]tree.StatRange)
			for _, topic := range c.topics {
				ranges, err := tmgr.GetStatisticsLatest(ctx, start, end, "my-device", topic, c.granularity)
				require.NoError(t, err)
				result[topic] = ranges
			}
			require.Equal(t, c.ranges, result)
		})
	}
}

func TestGetMessagesLatest(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion      string
		input          [][]uint64
		topics         []string
		bounds         []uint64
		outputMessages map[string][]uint64
	}{
		{
			"single topic file",
			[][]uint64{{10, 100, 1000}},
			[]string{"topic-0"},
			[]uint64{0, 1001},
			map[string][]uint64{
				"topic-0": {10, 100, 1000},
			},
		},
		{
			"exhibits [) behavior",
			[][]uint64{{10, 100, 1000}},
			[]string{"topic-0"},
			[]uint64{10, 1000},
			map[string][]uint64{
				"topic-0": {10, 100},
			},
		},
		{
			"respects lower bound",
			[][]uint64{{10, 100, 1000}},
			[]string{"topic-0"},
			[]uint64{100, 1001},
			map[string][]uint64{
				"topic-0": {100, 1000},
			},
		},
		{
			"topic that does not exist",
			[][]uint64{{10, 100, 1000}},
			[]string{"topic-1"},
			[]uint64{100, 1001},
			map[string][]uint64{},
		},
		{
			"multiple topics, one queried",
			[][]uint64{{10, 100, 1000}, {15, 200, 2000}},
			[]string{"topic-1"},
			[]uint64{0, 1001},
			map[string][]uint64{
				"topic-1": {15, 200},
			},
		},
		{
			"multiple topics, two queried, one not existing",
			[][]uint64{{10, 100, 1000}, {15, 200, 2000}},
			[]string{"topic-1", "topic-2"},
			[]uint64{0, 1001},
			map[string][]uint64{
				"topic-1": {15, 200},
			},
		},
		{
			"multiple topics, three queried",
			[][]uint64{{10, 100, 1000}, {15, 200, 2000}, {20, 300, 3000}},
			[]string{"topic-0", "topic-1", "topic-2"},
			[]uint64{0, 1001},
			map[string][]uint64{
				"topic-0": {10, 100, 1000},
				"topic-1": {15, 200},
				"topic-2": {20, 300},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			mcap.WriteFile(t, buf, c.input...)
			tmgr := testTreeManager(ctx, t)
			require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
			require.NoError(t, tmgr.SyncWAL(ctx))

			output := &bytes.Buffer{}
			start := c.bounds[0]
			end := c.bounds[1]
			require.NoError(t, tmgr.GetMessagesLatest(ctx, output, start, end, "my-device", c.topics))

			reader, err := mcap.NewReader(bytes.NewReader(output.Bytes()))
			require.NoError(t, err)
			messages := make(map[string][]uint64)
			it, err := reader.Messages()
			require.NoError(t, err)
			for {
				_, channel, message, err := it.Next(nil)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				messages[channel.Topic] = append(messages[channel.Topic], message.LogTime)
			}
			require.Equal(t, c.outputMessages, messages)
		})
	}
}

func TestSyncWAL(t *testing.T) {
}

func removeSpace(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "  ", "")
	s = strings.ReplaceAll(s, "\t", "")
	return s
}

func assertEqualTrees(t *testing.T, a, b string) {
	t.Helper()
	require.Equal(t, removeSpace(a), removeSpace(b), "%s != %s", a, b)
}

func BenchmarkReceive(b *testing.B) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		inputfile string
	}{
		{
			"cal loop",
			"/home/wyatt/data/bags/cal_loop.mcap",
		},
	}

	for _, c := range cases {
		b.Run(c.assertion, func(b *testing.B) {
			f, err := os.Open(c.inputfile)
			require.NoError(b, err)
			data, err := io.ReadAll(f)
			require.NoError(b, err)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tmgr := testTreeManager(ctx, b)
				require.NoError(b, tmgr.Receive(ctx, "my-device", bytes.NewReader(data)))
				require.NoError(b, f.Close())
			}
		})
	}
}

func TestReceive(t *testing.T) {
	ctx := context.Background()
	buf := &bytes.Buffer{}

	cases := []struct {
		assertion string
		input     [][]uint64
		output    []string
	}{
		{
			"single-topic file, single message",
			[][]uint64{{10}},
			[]string{
				`[0-64424509440:4 [0-1006632960:4 (count=1) [0-15728640:4 (count=1)
				[0-245760:4 (count=1) [0-3840:4 (count=1) [0-60:4 (count=1) [leaf 1 msg]]]]]]]`,
			},
		},
		{
			"two topics, single messages, nonoverlapping",
			[][]uint64{{10e9}, {100e9}},
			[]string{
				`[0-64424509440:6 [0-1006632960:6 (count=1) [0-15728640:6 (count=1) [0-245760:6 (count=1)
				[0-3840:6 (count=1) [0-60:6 (count=1) [leaf 1 msg]]]]]]]`,
				`[0-64424509440:7 [0-1006632960:7 (count=1) [0-15728640:7 (count=1)
				[0-245760:7 (count=1) [0-3840:7 (count=1) [60-120:7 (count=1) [leaf 1 msg]]]]]]]`,
			},
		},
		{
			"single-topic file, spanning leaf boundaries",
			[][]uint64{{10e9, 100e9}},
			[]string{
				`[0-64424509440:5 [0-1006632960:5 (count=2) [0-15728640:5 (count=2)
				[0-245760:5 (count=2) [0-3840:5 (count=2) [0-60:5 (count=1) [leaf 1 msg]]
				[60-120:5 (count=1) [leaf 1 msg]]]]]]]`,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf.Reset()
			mcap.WriteFile(t, buf, c.input...)
			tmgr := testTreeManager(ctx, t)
			require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
			require.NoError(t, tmgr.SyncWAL(ctx))

			for i := range c.output {
				topic := fmt.Sprintf("topic-%d", i)
				t.Run("comparing"+topic, func(t *testing.T) {
					str := tmgr.PrintStream(ctx, "my-device", topic)
					assertEqualTrees(t, c.output[i], str)
				})
			}
		})
	}
}

func testTreeManager(ctx context.Context, tb testing.TB) *treemgr.TreeManager {
	tb.Helper()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(tb, err)
	db.SetMaxOpenConns(1)
	wal, err := nodestore.NewSQLWAL(ctx, db)
	require.NoError(tb, err)
	ns := nodestore.NewNodestore(store, cache, wal)
	vs := versionstore.NewMemVersionStore()
	rm := rootmap.NewMemRootmap()
	tmgr := treemgr.NewTreeManager(ns, vs, rm, 2, 1)
	return tmgr
}
