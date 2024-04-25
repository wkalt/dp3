package treemgr_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/treemgr"
)

// nolint:dupl
func TestGetStatisticsLatest(t *testing.T) {
	ctx := context.Background()
	testhash := "1ba234e59378bc656d587c45c4191bfc24c2c657e871f148faa552350738c470"
	cases := []struct {
		assertion   string
		input       [][]int64
		granularity uint64
		topics      []string
		bounds      []uint64
		ranges      map[string][]nodestore.StatRange
	}{
		{
			"single topic file",
			[][]int64{{10, 100, 1000}},
			600 * 1e9,
			[]string{"topic-0"},
			[]uint64{0, 1001},
			map[string][]nodestore.StatRange{
				"topic-0": {
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Text, "data", "min", "hello"),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Text, "data", "max", "hello"),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "mean", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "min", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "max", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "sum", float64(6072)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "messageCount", int64(3)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "bytesUncompressed", int64(33)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "minObservedTime", int64(10)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "maxObservedTime", int64(1000)),
				},
			},
		},

		{
			"multiple buckets",
			[][]int64{{10, 100, 1000, 90e9}},
			600 * 1e9,
			[]string{"topic-0"},
			[]uint64{0, 100e9},
			map[string][]nodestore.StatRange{
				"topic-0": {
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Text, "data", "min", "hello"),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Text, "data", "max", "hello"),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "mean", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "min", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "max", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "sum", float64(6072)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "messageCount", int64(3)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "bytesUncompressed", int64(33)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "minObservedTime", int64(10)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "maxObservedTime", int64(1000)),

					nodestore.NewStatRange(testhash, 60e9, 120e9, nodestore.Text, "data", "min", "hello"),
					nodestore.NewStatRange(testhash, 60e9, 120e9, nodestore.Text, "data", "max", "hello"),
					nodestore.NewStatRange(testhash, 60e9, 120e9, nodestore.Float, "count", "mean", float64(2024)),
					nodestore.NewStatRange(testhash, 60e9, 120e9, nodestore.Float, "count", "min", float64(2024)),
					nodestore.NewStatRange(testhash, 60e9, 120e9, nodestore.Float, "count", "max", float64(2024)),
					nodestore.NewStatRange(testhash, 60e9, 120e9, nodestore.Float, "count", "sum", float64(2024)),
					nodestore.NewStatRange(testhash, 60e9, 120e9, nodestore.Int, "", "messageCount", int64(1)),
					nodestore.NewStatRange(testhash, 60e9, 120e9, nodestore.Int, "", "bytesUncompressed", int64(11)),
					nodestore.NewStatRange(testhash, 60e9, 120e9, nodestore.Int, "", "minObservedTime", int64(90e9)),
					nodestore.NewStatRange(testhash, 60e9, 120e9, nodestore.Int, "", "maxObservedTime", int64(90e9)),
				},
			},
		},
		{
			"multiple buckets, low granularity",
			[][]int64{{10, 100, 1000, 90e9}},
			64 * 600 * 1e9,
			[]string{"topic-0"},
			[]uint64{0, 100e9},
			map[string][]nodestore.StatRange{
				"topic-0": {
					nodestore.NewStatRange(testhash, 0, 3840e9, nodestore.Text, "data", "min", "hello"),
					nodestore.NewStatRange(testhash, 0, 3840e9, nodestore.Text, "data", "max", "hello"),
					nodestore.NewStatRange(testhash, 0, 3840e9, nodestore.Float, "count", "mean", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 3840e9, nodestore.Float, "count", "min", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 3840e9, nodestore.Float, "count", "max", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 3840e9, nodestore.Float, "count", "sum", float64(8096)),
					nodestore.NewStatRange(testhash, 0, 3840e9, nodestore.Int, "", "messageCount", int64(4)),
					nodestore.NewStatRange(testhash, 0, 3840e9, nodestore.Int, "", "bytesUncompressed", int64(44)),
					nodestore.NewStatRange(testhash, 0, 3840e9, nodestore.Int, "", "minObservedTime", int64(10)),
					nodestore.NewStatRange(testhash, 0, 3840e9, nodestore.Int, "", "maxObservedTime", int64(90e9)),
				},
			},
		},
		{
			"excludes buckets based on [) semantics",
			[][]int64{{10, 100, 1000, 90e9}},
			600 * 1e9,
			[]string{"topic-0"},
			[]uint64{100, 60e9},
			map[string][]nodestore.StatRange{
				"topic-0": {
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Text, "data", "min", "hello"),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Text, "data", "max", "hello"),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "mean", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "min", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "max", float64(2024)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Float, "count", "sum", float64(6072)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "messageCount", int64(3)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "bytesUncompressed", int64(33)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "minObservedTime", int64(10)),
					nodestore.NewStatRange(testhash, 0, 60e9, nodestore.Int, "", "maxObservedTime", int64(1000)),
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			mcap.WriteFile(t, buf, c.input...)
			tmgr, finish := treemgr.TestTreeManager(ctx, t)
			defer finish()
			require.NoError(t, tmgr.Receive(ctx, "db", "my-device", buf))
			require.NoError(t, tmgr.ForceFlush(ctx))
			start := c.bounds[0]
			end := c.bounds[1]

			result := make(map[string][]nodestore.StatRange)
			for _, topic := range c.topics {
				ranges, err := tmgr.GetStatisticsLatest(ctx, "db", "my-device", topic, start, end, c.granularity)
				require.NoError(t, err)
				result[topic] = ranges
			}
			require.Equal(t, c.ranges, result)
		})
	}
}

func TestGetTables(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		input     [][]int64

		historical bool
		topic      string
		producer   string
		counts     map[string]int64
	}{
		{
			"single topic file",
			[][]int64{{10, 100, 1000}},

			false,
			"topic-0",
			"my-device",
			map[string]int64{
				"topic-0": 3,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			mcap.WriteFile(t, buf, c.input...)
			tmgr, finish := treemgr.TestTreeManager(ctx, t)
			defer finish()
			require.NoError(t, tmgr.Receive(ctx, "db", "my-device", buf))
			require.NoError(t, tmgr.ForceFlush(ctx))

			tables, err := tmgr.GetTables(ctx, "db", c.producer, c.topic, c.historical)
			require.NoError(t, err)

			m := make(map[string]int64)
			for _, table := range tables {
				for _, child := range table.Children {
					if child == nil {
						continue
					}
					for _, stats := range child.Statistics {
						m[table.Root.Topic] += stats.MessageCount
					}
				}
			}

			require.Equal(t, c.counts, m)
		})
	}
}

func TestGetMessages(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion      string
		input          [][]int64
		topics         []string
		bounds         []uint64
		outputMessages map[string][]uint64
	}{
		{
			"single topic file",
			[][]int64{{10, 100, 1000}},
			[]string{"topic-0"},
			[]uint64{0, 1001},
			map[string][]uint64{
				"topic-0": {10, 100, 1000},
			},
		},
		{
			"exhibits [) behavior",
			[][]int64{{10, 100, 1000}},
			[]string{"topic-0"},
			[]uint64{10, 1000},
			map[string][]uint64{
				"topic-0": {10, 100},
			},
		},
		{
			"respects lower bound",
			[][]int64{{10, 100, 1000}},
			[]string{"topic-0"},
			[]uint64{100, 1001},
			map[string][]uint64{
				"topic-0": {100, 1000},
			},
		},
		{
			"topic that does not exist",
			[][]int64{{10, 100, 1000}},
			[]string{"topic-1"},
			[]uint64{100, 1001},
			map[string][]uint64{},
		},
		{
			"multiple topics, one queried",
			[][]int64{{10, 100, 1000}, {15, 200, 2000}},
			[]string{"topic-1"},
			[]uint64{0, 1001},
			map[string][]uint64{
				"topic-1": {15, 200},
			},
		},
		{
			"multiple topics, two queried, one not existing",
			[][]int64{{10, 100, 1000}, {15, 200, 2000}},
			[]string{"topic-1", "topic-2"},
			[]uint64{0, 1001},
			map[string][]uint64{
				"topic-1": {15, 200},
			},
		},
		{
			"two topics, both queried, one with data in leaf but not in requested range",
			[][]int64{{10, 100, 1000}, {15, 200, 2000}},
			[]string{"topic-0", "topic-1"},
			[]uint64{13, 16},
			map[string][]uint64{
				"topic-1": {15},
			},
		},
		{
			"multiple topics, three queried",
			[][]int64{{10, 100, 1000}, {15, 200, 2000}, {20, 300, 3000}},
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
			tmgr, finish := treemgr.TestTreeManager(ctx, t)
			defer finish()
			require.NoError(t, tmgr.Receive(ctx, "db", "my-device", buf))
			require.NoError(t, tmgr.ForceFlush(ctx))

			output := &bytes.Buffer{}
			start := c.bounds[0]
			end := c.bounds[1]
			topics := map[string]uint64{}
			for _, topic := range c.topics {
				topics[topic] = 0
			}
			roots, err := tmgr.GetLatestRoots(ctx, "db", "my-device", topics)
			require.NoError(t, err)

			require.NoError(t, tmgr.GetMessages(ctx, output, start, end, roots))

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
	mcap.WriteFile(t, buf, []int64{10e9})

	tmgr, finish := treemgr.TestTreeManager(ctx, t)
	defer finish()
	require.NoError(t, tmgr.Receive(ctx, "db", "my-device", buf))
	require.NoError(t, tmgr.ForceFlush(ctx))

	// overlapping
	buf.Reset()
	mcap.WriteFile(t, buf, []int64{10e9})
	require.NoError(t, tmgr.Receive(ctx, "db", "my-device", buf))
	require.NoError(t, tmgr.ForceFlush(ctx))

	// nonoverlapping
	buf.Reset()
	mcap.WriteFile(t, buf, []int64{1000e9})
	require.NoError(t, tmgr.Receive(ctx, "db", "my-device", buf))
	require.NoError(t, tmgr.ForceFlush(ctx))

	output := &bytes.Buffer{}
	roots, err := tmgr.GetLatestRoots(ctx, "db", "my-device", map[string]uint64{"topic-0": 0})
	require.NoError(t, err)

	require.NoError(t, tmgr.GetMessages(ctx, output, 0, 100000e9, roots))

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

func TestReceiveDifferentSchemas(t *testing.T) {
	ctx := context.Background()
	t.Run("field added", func(t *testing.T) {
		tmgr, finish := treemgr.TestTreeManager(ctx, t)
		defer finish()

		buf := &bytes.Buffer{}

		mcap.WriteFileExtended(t, buf, 1, []int64{10e9})
		require.NoError(t, tmgr.Receive(ctx, "db", "my-device", buf))
		require.NoError(t, tmgr.ForceFlush(ctx))

		buf.Reset()

		mcap.WriteFileExtended(t, buf, 2, []int64{100e9})
		require.NoError(t, tmgr.Receive(ctx, "db", "my-device", buf))
		require.NoError(t, tmgr.ForceFlush(ctx))

		expected := `[0-64424509440 [0-1006632960:5 (12 count=1 13 count=1)
		[0-15728640:5 (12 count=1 13 count=1) [0-245760:5 (12 count=1 13 count=1) [0-3840:5 (12 count=1 13 count=1)
		[0-60:3 (13 count=1) [leaf 1 msg]] [60-120:5 (12 count=1) [leaf 1 msg]]]]]]]`

		str := tmgr.PrintTable(ctx, "db", "my-device", "topic-0")
		assertEqualTrees(t, expected, str)
	})
}

func runSequence(ctx context.Context, t *testing.T, tmgr *treemgr.TreeManager, s string) string {
	t.Helper()
	buf := &bytes.Buffer{}
	for len(s) > 0 {
		buf.Reset()
		switch s[0] {
		case ' ':
			s = s[1:]
			continue
		case 'w': // w(10, 100)
			nums := strings.Split(s[2:strings.Index(s, ")")], ",")
			left, err := strconv.ParseInt(nums[0], 10, 64)
			require.NoError(t, err)
			right, err := strconv.ParseInt(nums[1], 10, 64)
			require.NoError(t, err)
			mcap.WriteFile(t, buf, [][]int64{{1e9 * left, 1e9 * right}}...)
			require.NoError(t, tmgr.Receive(ctx, "db", "my-device", buf))
		case 'd':
			nums := strings.Split(s[2:strings.Index(s, ")")], ",")
			left, err := strconv.ParseInt(nums[0], 10, 64)
			require.NoError(t, err)
			right, err := strconv.ParseInt(nums[1], 10, 64)
			require.NoError(t, err)
			mcap.WriteFile(t, buf, [][]int64{{left}, {right}}...)
			require.NoError(t, tmgr.DeleteMessages(
				ctx, "db", "my-device", "topic-0", 1e9*uint64(left), 1e9*uint64(right)))
		default:
			require.Fail(t, "unknown command")
		}
		require.NoError(t, tmgr.ForceFlush(ctx))
		s = s[strings.Index(s, ")")+1:]
	}
	return tmgr.PrintTable(ctx, "db", "my-device", "topic-0")
}

// NB: this is really a tree iterator test, but the treemgr machinery is useful
// for constructing detailed test scenarios. Maybe we can figure out a way to
// extract it.
func TestTreeIteration(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		input     string
		messages  []uint64
	}{
		{
			"single write",
			"w(10,50)",
			[]uint64{10, 50},
		},
		{
			"two writes on two leaves",
			"w(10,50) w(60,100)",
			[]uint64{10, 50, 60, 100},
		},
		{
			"two writes two leaves out of order",
			"w(60,100) w(10,50)",
			[]uint64{10, 50, 60, 100},
		},
		{
			"two writes, overlapping, in order",
			"w(10,50) w(40,70)",
			[]uint64{10, 40, 50, 70},
		},
		{
			"two writes, overlapping, two leaves, out of order",
			"w(40,70) w(10,50)",
			[]uint64{10, 40, 50, 70},
		},
		{
			"two overlapping writes on single leaf",
			"w(10,50) w(5,15)",
			[]uint64{5, 10, 15, 50},
		},
		{
			"two adjacent writes on a single leaf",
			"w(5,10) w(15,20)",
			[]uint64{5, 10, 15, 20},
		},
		{
			"adjacent, out of order writes on single leaf",
			"w(15,20) w(5,10)",
			[]uint64{5, 10, 15, 20},
		},
		{
			"partial delete covering the left side of a write",
			"w(10,50) d(10,20)",
			[]uint64{50},
		},
		{
			"partial delete from the middle of a write, deleting no messages",
			"w(10,50) d(20,30)",
			[]uint64{10, 50},
		},
		{
			"partial delete covering right side of a write",
			"w(10,50) d(40,51)",
			[]uint64{10},
		},
		{
			"deletion adheres to [) semantics",
			"w(10,50) d(40,50)",
			[]uint64{10, 50},
		},
		{
			"write delete write",
			"w(10,50) d(40,70) w(60,100)",
			[]uint64{10, 60, 100},
		},
		{
			"write, delete, write, delete",
			"w(10,50) d(40,70) w(60,100) d(90,120)",
			[]uint64{10, 60},
		},
		{
			"write, delete, delete",
			"w(10,50) d(5,20) d(40,60)",
			[]uint64{},
		},
		{
			"delete from covered middle of a range",
			"w(10,20) w(30,40) w(60,80) d(15,35)",
			[]uint64{10, 40, 60, 80},
		},
		{
			"delete spanning multiple pages",
			"w(10,20) w(30,40) w(60,80) d(15,65)",
			[]uint64{10, 80},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			tmgr, finish := treemgr.TestTreeManager(ctx, t)
			defer finish()
			runSequence(ctx, t, tmgr, c.input)

			output := &bytes.Buffer{}
			roots, err := tmgr.GetLatestRoots(
				ctx, "db", "my-device", map[string]uint64{"topic-0": 0},
			)
			require.NoError(t, err)

			require.NoError(t, tmgr.GetMessages(ctx, output, 0, ^uint64(0), roots))

			reader, err := mcap.NewReader(bytes.NewReader(output.Bytes()))
			require.NoError(t, err)

			info, err := reader.Info()
			require.NoError(t, err)
			require.Equal(t, len(c.messages), int(info.Statistics.MessageCount))

			messages := []uint64{}
			it, err := reader.Messages()
			require.NoError(t, err)
			for {
				_, _, message, err := it.Next(nil)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				messages = append(messages, message.LogTime/1e9)
			}
			require.Equal(t, c.messages, messages)
		})
	}
}

func TestReceive(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		assertion string
		input     [][]int64
		output    []string
	}{
		{
			"single-topic file, single message",
			[][]int64{{10e9}},
			[]string{
				`[0-64424509440 [0-1006632960:3 (1b count=1) [0-15728640:3 (1b count=1)
				[0-245760:3 (1b count=1) [0-3840:3 (1b count=1) [0-60:3 (1b count=1) [leaf 1 msg]]]]]]]`,
			},
		},
		{
			"two topics, single messages, nonoverlapping",
			[][]int64{{10e9}, {100e9}},
			[]string{
				`[0-64424509440 [0-1006632960:4 (1b count=1) [0-15728640:4 (1b count=1)
				[0-245760:4 (1b count=1) [0-3840:4 (1b count=1) [0-60:4 (1b count=1) [leaf 1 msg]]]]]]]`,
				`[0-64424509440 [0-1006632960:5 (1b count=1) [0-15728640:5 (1b count=1)
				[0-245760:5 (1b count=1) [0-3840:5 (1b count=1) [60-120:5 (1b count=1) [leaf 1 msg]]]]]]]`,
			},
		},
		{
			"single-topic file, spanning leaf boundaries",
			[][]int64{{10e9, 100e9}},
			[]string{
				`[0-64424509440 [0-1006632960:4 (1b count=2) [0-15728640:4 (1b count=2)
				[0-245760:4 (1b count=2) [0-3840:4 (1b count=2) [0-60:3 (1b count=1) [leaf 1 msg]]
				[60-120:4 (1b count=1) [leaf 1 msg]]]]]]]`,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			buf.Reset()
			mcap.WriteFile(t, buf, c.input...)
			tmgr, finish := treemgr.TestTreeManager(ctx, t)
			defer finish()
			require.NoError(t, tmgr.Receive(ctx, "db", "my-device", buf))
			require.NoError(t, tmgr.ForceFlush(ctx))

			for i := range c.output {
				topic := fmt.Sprintf("topic-%d", i)
				t.Run("comparing"+topic, func(t *testing.T) {
					str := tmgr.PrintTable(ctx, "db", "my-device", topic)
					assertEqualTrees(t, c.output[i], str)
				})
			}
		})
	}
}
