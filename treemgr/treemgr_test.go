package treemgr_test

import (
	"bytes"
	"context"
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
			tmgr, finish := testTreeManager(ctx, t)
			defer finish()
			require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
			require.NoError(t, tmgr.ForceFlush(ctx))
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
			tmgr, finish := testTreeManager(ctx, t)
			defer finish()
			require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
			require.NoError(t, tmgr.ForceFlush(ctx))

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

func TestStreamingAcrossMultipleReceives(t *testing.T) {
	ctx := context.Background()
	buf := &bytes.Buffer{}
	mcap.WriteFile(t, buf, []uint64{10e9})

	tmgr, finish := testTreeManager(ctx, t)
	defer finish()
	require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
	require.NoError(t, tmgr.ForceFlush(ctx))

	// overlapping
	buf.Reset()
	mcap.WriteFile(t, buf, []uint64{10e9})
	require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
	require.NoError(t, tmgr.ForceFlush(ctx))

	// nonoverlapping
	buf.Reset()
	mcap.WriteFile(t, buf, []uint64{1000e9})
	require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
	require.NoError(t, tmgr.ForceFlush(ctx))

	output := &bytes.Buffer{}
	require.NoError(t, tmgr.GetMessagesLatest(ctx, output, 0, 100000e9, "my-device", []string{"topic-0"}))

	reader, err := mcap.NewReader(bytes.NewReader(output.Bytes()))
	require.NoError(t, err)

	info, err := reader.Info()
	require.NoError(t, err)
	require.Equal(t, 3, int(info.Statistics.MessageCount))

	require.Len(t, info.Schemas, 1)
	for _, channel := range info.Channels {
		require.Equal(t, "topic-0", channel.Topic)
		schema := info.Schemas[channel.SchemaID]
		require.NotNil(t, schema)
	}
}

func TestReceive(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		input     [][]uint64
		output    []string
	}{
		{
			"single-topic file, single message",
			[][]uint64{{10e9}},
			[]string{
				`[0-64424509440 [0-1006632960:3 (count=1) [0-15728640:3 (count=1)
				[0-245760:3 (count=1) [0-3840:3 (count=1) [0-60:3 (count=1) [leaf 1 msg]]]]]]]`,
			},
		},
		{
			"two topics, single messages, nonoverlapping",
			[][]uint64{{10e9}, {100e9}},
			[]string{
				`[0-64424509440 [0-1006632960:4 (count=1) [0-15728640:4 (count=1)
				[0-245760:4 (count=1) [0-3840:4 (count=1) [0-60:4 (count=1) [leaf 1 msg]]]]]]]`,
				`[0-64424509440 [0-1006632960:5 (count=1) [0-15728640:5 (count=1)
				[0-245760:5 (count=1) [0-3840:5 (count=1) [60-120:5 (count=1) [leaf 1 msg]]]]]]]`,
			},
		},
		{
			"single-topic file, spanning leaf boundaries",
			[][]uint64{{10e9, 100e9}},
			[]string{
				`[0-64424509440 [0-1006632960:4 (count=2) [0-15728640:4 (count=2)
				[0-245760:4 (count=2) [0-3840:4 (count=2) [0-60:3 (count=1) [leaf 1 msg]]
				[60-120:4 (count=1) [leaf 1 msg]]]]]]]`,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			buf.Reset()
			mcap.WriteFile(t, buf, c.input...)
			tmgr, finish := testTreeManager(ctx, t)
			defer finish()
			require.NoError(t, tmgr.Receive(ctx, "my-device", buf))
			require.NoError(t, tmgr.ForceFlush(ctx))

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

func testTreeManager(ctx context.Context, tb testing.TB) (*treemgr.TreeManager, func()) {
	tb.Helper()
	store := storage.NewMemStore()
	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1000)
	ns := nodestore.NewNodestore(store, cache)
	vs := versionstore.NewMemVersionStore()
	rm := rootmap.NewMemRootmap()
	tmpdir, err := os.MkdirTemp("", "dp3-test")
	require.NoError(tb, err)
	tmgr, err := treemgr.NewTreeManager(
		ctx,
		ns,
		vs,
		rm,
		treemgr.WithWALBufferSize(100),
		treemgr.WithSyncWorkers(0), // control syncing manually
		treemgr.WithWALDir(tmpdir),
	)
	require.NoError(tb, err)
	return tmgr, func() {
		os.RemoveAll(tmpdir)
	}
}
