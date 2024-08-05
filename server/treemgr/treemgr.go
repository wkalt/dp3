package treemgr

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/nodestore"
	"github.com/wkalt/dp3/server/rootmap"
	"github.com/wkalt/dp3/server/schemastore"
	"github.com/wkalt/dp3/server/tree"
	"github.com/wkalt/dp3/server/util"
	"github.com/wkalt/dp3/server/util/log"
	"github.com/wkalt/dp3/server/versionstore"
	"github.com/wkalt/dp3/server/wal"
	"golang.org/x/sync/errgroup"
)

/*
The tree manager oversees the set of trees involved in data storage. One tree is
associated with each (producer, topic) pair, so the total number of trees under
management can grow very large.
*/

////////////////////////////////////////////////////////////////////////////////

// ErrNotImplemented is returned when a method is not implemented.
var ErrNotImplemented = errors.New("not implemented")

// TreeManager is the main interface to the treemgr package.
type TreeManager struct {
	ns      *nodestore.Nodestore
	ss      *schemastore.SchemaStore
	vs      *versionstore.VersionStore
	rootmap rootmap.Rootmap
	merges  <-chan *wal.Batch

	syncWorkers int

	wal *wal.Manager
}

// NewRoot creates a new root for the provided producer and topic.
func (tm *TreeManager) NewRoot(ctx context.Context, database string, producer string, topic string) error {
	var slashReplacementChar = "!()!"
	safeProducer := strings.ReplaceAll(producer, "/", slashReplacementChar)
	safeTopic := strings.ReplaceAll(topic, "/", slashReplacementChar)
	safeDatabase := strings.ReplaceAll(database, "/", slashReplacementChar)
	prefix := fmt.Sprintf("%s/tables/%s/%s", safeDatabase, safeTopic, safeProducer)

	rootID, version, err := tm.newRoot(
		ctx,
		prefix,
		util.DateSeconds("1970-01-01"),
		util.DateSeconds("2038-01-19"),
		60,
		64,
	)
	if err != nil {
		return fmt.Errorf("failed to create new root: %w", err)
	}
	if err := tm.rootmap.Put(ctx, database, producer, topic, version, prefix, rootID); err != nil {
		return fmt.Errorf("failed to put root: %w", err)
	}
	return nil
}

// NewTreeManager returns a new TreeManager.
func NewTreeManager(
	ctx context.Context,
	ns *nodestore.Nodestore,
	ss *schemastore.SchemaStore,
	vs *versionstore.VersionStore,
	rm rootmap.Rootmap,
	opts ...Option,
) (*TreeManager, error) {
	conf := config{
		walBufferSize: 0,
		syncWorkers:   1,
		waldir:        "",
	}
	for _, opt := range opts {
		opt(&conf)
	}
	if conf.waldir == "" {
		return nil, errors.New("wal directory not specified")
	}
	merges := make(chan *wal.Batch, conf.walBufferSize)
	wmgr, err := wal.NewWALManager(ctx, conf.waldir, merges, conf.walopts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL manager: %w", err)
	}

	tm := &TreeManager{
		ns:          ns,
		ss:          ss,
		vs:          vs,
		wal:         wmgr,
		merges:      merges,
		rootmap:     rm,
		syncWorkers: conf.syncWorkers,
	}

	// start listeners on the merges channel that will multiplex merge requests
	// over a pool of workers, arranged so that each producer/topic pair is
	// routed to a single worker only.
	if conf.syncWorkers > 0 {
		go tm.spawnWALConsumers(ctx)
	}

	// Recover the WAL. This will feed the merges channel with any uncommitted
	// work.
	if err := wmgr.Recover(ctx); err != nil {
		return nil, fmt.Errorf("failed to recover: %w", err)
	}

	// ready for requests
	return tm, nil
}

func (tm *TreeManager) Producers(
	ctx context.Context, database string, topics []string,
) ([]string, error) {
	producers, err := tm.rootmap.Producers(ctx, database, topics)
	if err != nil {
		return nil, fmt.Errorf("failed to get producers: %w", err)
	}
	return producers, nil
}

func (tm *TreeManager) Databases(ctx context.Context) ([]string, error) {
	databases, err := tm.rootmap.Databases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get databases: %w", err)
	}
	return databases, nil
}

func (tm *TreeManager) Topics(
	ctx context.Context, database string,
) ([]string, error) {
	topics, err := tm.rootmap.Topics(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("failed to get topics: %w", err)
	}
	return topics, nil
}

func (tm *TreeManager) DeleteMessages(
	ctx context.Context,
	database string,
	producer string,
	topic string,
	start, end uint64,
) error {
	prefix, nodeID, _, _, err := tm.rootmap.GetLatest(ctx, database, producer, topic)
	if err != nil {
		return fmt.Errorf("failed to get latest root: %w", err)
	}
	root, err := tm.ns.Get(ctx, prefix, nodeID)
	if err != nil {
		return fmt.Errorf("failed to get root: %w", err)
	}
	version, err := tm.vs.NextVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}
	inner, ok := root.(*nodestore.InnerNode)
	if !ok {
		return fmt.Errorf("unexpected node type: %w", tree.NewUnexpectedNodeError(nodestore.Inner, root))
	}
	mt, err := tree.NewDelete(ctx, inner, version, start, end)
	if err != nil {
		return fmt.Errorf("failed to delete messages: %w", err)
	}
	serialized, err := mt.ToBytes(ctx, 0)
	if err != nil {
		return fmt.Errorf("failed to serialize tree: %w", err)
	}
	_, err = tm.wal.Insert(database, producer, topic, nil, serialized)
	if err != nil {
		return fmt.Errorf("failed to insert into WAL: %w", err)
	}
	return nil
}

// Receive an MCAP data stream on behalf of a particular producer. The data is
// split by topic and hashed with the producer ID, to result in one tree in
// storage per topic per producer. After Receive returns, the data is in the
// WAL, not in final storage. To get to final storage, a call to SyncWAL must
// occur, which performs a merge of pending partial trees in the WAL and writes
// a single object to storage, in the interest of making write sizes independent
// of the input data sizes.
func (tm *TreeManager) Receive(
	ctx context.Context,
	database string,
	producer string,
	data io.Reader,
) error {
	writers := map[string]*writer{}
	reader, err := mcap.NewReader(data)
	if err != nil {
		return fmt.Errorf("failed to create mcap reader: %w", err)
	}
	defer reader.Close()
	it, err := reader.Messages(fmcap.UsingIndex(false), fmcap.InOrder(fmcap.FileOrder))
	if err != nil {
		return fmt.Errorf("failed to create message iterator: %w", err)
	}
	buf := make([]byte, 1024)
	msg := &fmcap.Message{}
	schemas := make(map[uint16]*fmcap.Schema)
	for {
		schema, channel, msg, err := it.NextInto(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next message: %w", err)
		}
		if _, ok := schemas[schema.ID]; !ok {
			schemas[schema.ID] = schema
		}
		if len(msg.Data) > len(buf) {
			buf = make([]byte, 2*len(msg.Data))
		}
		var writer *writer
		var ok bool
		if writer, ok = writers[channel.Topic]; !ok {
			var finish func()
			writer, finish, err = tm.handleNewTopic(ctx, database, producer, channel.Topic)
			if err != nil {
				return fmt.Errorf("failed to create writer: %w", err)
			}
			writers[channel.Topic] = writer
			defer finish()
		}
		if err := writer.Write(ctx, schema, channel, msg); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
	}
	for _, schema := range schemas {
		if err := tm.storeSchema(ctx, database, schema); err != nil {
			return fmt.Errorf("failed to store schema: %w", err)
		}
	}
	for _, k := range util.Okeys(writers) {
		writer := writers[k]
		if err := writer.Close(ctx); err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}
	}
	return nil
}

func (tm *TreeManager) handleNewTopic(
	ctx context.Context,
	database string,
	producer string,
	topic string,
) (*writer, func(), error) {
	if _, _, _, _, err := tm.rootmap.GetLatest(ctx, database, producer, topic); err != nil {
		switch {
		case errors.Is(err, rootmap.TableNotFoundError{}):
			if err := tm.NewRoot(ctx, database, producer, topic); err != nil &&
				!errors.Is(err, rootmap.ErrRootAlreadyExists) {
				return nil, nil, fmt.Errorf("failed to create new root: %w", err)
			}
		default:
			return nil, nil, fmt.Errorf("failed to get latest root: %w", err)
		}
	}
	compressor, ok := compressorPool.Get().(fmcap.CustomCompressor)
	if !ok {
		return nil, nil, errors.New("failed to get compressor from pool")
	}
	writer, err := newWriter(ctx, tm, database, producer, topic, compressor)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create writer: %w", err)
	}
	finish := func() { compressorPool.Put(compressor) }
	return writer, finish, nil
}

// GetSchema retrieves a schema from the schema store.
func (tm *TreeManager) GetSchema(
	ctx context.Context,
	database string,
	hash string,
) (*fmcap.Schema, error) {
	schema, err := tm.ss.Get(ctx, database, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	return schema, nil
}

func (tm *TreeManager) storeSchema(ctx context.Context, database string, schema *fmcap.Schema) error {
	hash := util.CryptographicHash(schema.Data)
	_, err := tm.ss.Get(ctx, database, hash)
	if err != nil {
		if errors.Is(err, schemastore.ErrSchemaNotFound) {
			if err := tm.ss.Put(ctx, database, hash, schema); err != nil {
				return fmt.Errorf("failed to put schema: %w", err)
			}
			return nil
		}
		return fmt.Errorf("failed to get schema: %w", err)
	}
	return nil
}

// GetStatistics returns a summary of statistics for the given time range.
func (tm *TreeManager) GetStatistics(
	ctx context.Context,
	database string,
	producer string,
	topic string,
	start, end uint64,
	version uint64,
	granularity uint64,
) ([]nodestore.StatRange, error) {
	prefix, rootID, err := tm.rootmap.Get(ctx, database, producer, topic, version)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest root: %w", err)
	}
	tr := tree.NewBYOTreeReader(prefix, rootID, tm.ns.Get, tm.ns.GetLeafNode)
	ranges, err := tree.GetStatRange(ctx, tr, start, end, granularity)
	if err != nil {
		return nil, fmt.Errorf("failed to get stat range: %w", err)
	}
	return ranges, nil
}

type ChildSummary struct {
	Producer string `json:"producer"`
	Topic    string `json:"topic"`

	Start             time.Time `json:"start"`
	End               time.Time `json:"end"`
	MessageCount      uint64    `json:"messageCount"`
	BytesUncompressed uint64    `json:"bytesUncompressed"`
	MinObservedTime   time.Time `json:"minObservedTime"`
	MaxObservedTime   time.Time `json:"maxObservedTime"`
	SchemaHashes      []string  `json:"schemaHashes"`
}

type AvailableStatisticsRequest struct {
	Topic     string `json:"topic"`
	Producer  string `json:"producer"`
	StartSecs int    `json:"startSecs"`
	EndSecs   int    `json:"endSecs"`
}

type AvailableStatistic struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type AvailableStatisticsResponse struct {
	Field      string               `json:"field"`
	Type       string               `json:"type"`
	Statistics []AvailableStatistic `json:"statistics"`
}

type StatisticsResponse struct {
	Producers    []string         `json:"producers"`
	SchemaHashes []string         `json:"schemaHashes"`
	Breaks       []int            `json:"breaks"`
	Binwidth     int              `json:"binwidth"`
	Statistics   map[string][]any `json:"statistics"`
}

func (tm *TreeManager) Statistics( // nolint: funlen
	ctx context.Context,
	database string,
	topic string,
	producer *string,
	field string,
	stats []string,
	start, end int,
	granularity int,
	groupByProducer bool,
) ([]StatisticsResponse, error) {
	topics := map[string]uint64{topic: 0}
	var err error
	producers := []string{}
	if producer != nil {
		producers = append(producers, *producer)
	} else {
		producers, err = tm.rootmap.Producers(ctx, database, []string{topic})
		if err != nil {
			return nil, fmt.Errorf("failed to get producers: %w", err)
		}
	}
	// For each producer, if the producer has relevant fields, we will look at
	// the latest root and get a binned timeseries. Each producer is expected to
	// have the same binsizes within the topic. After all producers have been
	// checked, bins are merged and statistics are calculated.
	type bin struct {
		startSecs, endSecs int
	}
	var binWidth int
	fields := util.When(len(field) == 0, []string{}, []string{field})
	var groups [][]string
	if groupByProducer {
		for _, producer := range producers {
			groups = append(groups, []string{producer})
		}
	} else {
		groups = [][]string{producers}
	}
	responses := []StatisticsResponse{}
	for _, group := range groups {
		hashes := make(map[string]bool)
		bins := map[bin]*nodestore.ChildStats{}
		for _, producer := range group {
			roots, err := tm.rootmap.GetLatestByTopic(ctx, database, []string{producer}, topics)
			if err != nil {
				return nil, fmt.Errorf("failed to get latest roots: %w", err)
			}
			if len(roots) != 1 {
				return nil, fmt.Errorf("expected one root, got %d", len(roots))
			}
			root := roots[0]
			if err := tree.IterateChildren(
				ctx,
				tree.NewBYOTreeReader(root.Prefix, root.NodeID, tm.ns.Get, tm.ns.GetLeafNode),
				uint64(start),
				uint64(end),
				granularity,
				func(child *nodestore.Child, start uint64, end uint64) error {
					binWidth = int(end/1e9 - start/1e9)
					childstats, err := child.GetStats(fields)
					if err != nil {
						return fmt.Errorf("failed to get stats: %w", err)
					}
					b := bin{int(start / 1e9), int(end / 1e9)}
					if existing, ok := bins[b]; ok {
						if err := existing.Merge(childstats); err != nil {
							return fmt.Errorf("failed to merge stats: %w", err)
						}
						return nil
					}
					bins[b] = childstats
					for _, hash := range childstats.MessageSummary.SchemaHashes {
						hashes[hash] = true
					}
					return nil
				},
			); err != nil {
				return nil, fmt.Errorf("failed to iterate children: %w", err)
			}
		}

		keys := make([]bin, 0, len(bins))
		for k := range bins {
			keys = append(keys, k)
		}

		sort.Slice(keys, func(i, j int) bool {
			return keys[i].startSecs < keys[j].startSecs
		})

		breaks := make([]int, 0, len(keys))
		for _, k := range keys {
			breaks = append(breaks, k.startSecs)
		}
		response := StatisticsResponse{
			Producers:    group,
			SchemaHashes: util.Okeys(hashes),
			Breaks:       breaks,
			Statistics:   map[string][]any{},
			Binwidth:     binWidth,
		}

		// each bin is valued with an aggregated summary pair
		for _, k := range keys {
			childStats := bins[k]
			numeric := childStats.FieldStats[field].Numeric
			text := childStats.FieldStats[field].Text
			if err := addNumericStats(&response, stats, numeric); err != nil {
				return nil, fmt.Errorf("failed to add numeric stats: %w", err)
			}
			addTextStats(&response, stats, text)
			addMessageStats(&response, stats, childStats.MessageSummary)
		}
		responses = append(responses, response)
	}
	return responses, nil
}

func addMessageStats(response *StatisticsResponse, stats []string, messageSummary nodestore.MessageSummary) {
	for _, stat := range stats {
		switch stat {
		case "messageCount":
			response.Statistics[stat] = append(response.Statistics[stat], messageSummary.Count)
		case "messageBytesUncompressed":
			response.Statistics[stat] = append(response.Statistics[stat], messageSummary.BytesUncompressed)
		case "messageMinObservedTime":
			datestr := time.Unix(0, int64(messageSummary.MinObservedTime)).UTC().Format(time.RFC3339Nano)
			response.Statistics[stat] = append(response.Statistics[stat], datestr)
		case "messageMaxObservedTime":
			datestr := time.Unix(0, int64(messageSummary.MaxObservedTime)).UTC().Format(time.RFC3339Nano)
			response.Statistics[stat] = append(response.Statistics[stat], datestr)
		}
	}
}

func addTextStats(response *StatisticsResponse, stats []string, textstat *nodestore.TextSummary) {
	if textstat == nil {
		return
	}
	for _, stat := range stats {
		switch stat {
		case "min":
			response.Statistics[stat] = append(response.Statistics[stat], textstat.Min)
		case "max":
			response.Statistics[stat] = append(response.Statistics[stat], textstat.Max)
		}
	}
}

func addNumericStats(response *StatisticsResponse, stats []string, numstat *nodestore.NumericalSummary) error {
	if numstat == nil {
		return nil
	}
	quantiles, err := numstat.Quantiles([]float64{0.25, 0.5, 0.75, 0.9, 0.95, 0.99})
	if err != nil {
		return fmt.Errorf("failed to get quantiles: %w", err)
	}
	for _, stat := range stats {
		switch stat {
		case "min":
			response.Statistics[stat] = append(response.Statistics[stat], numstat.Min)
		case "max":
			response.Statistics[stat] = append(response.Statistics[stat], numstat.Max)
		case "mean":
			response.Statistics[stat] = append(response.Statistics[stat], numstat.Mean)
		case "count":
			response.Statistics[stat] = append(response.Statistics[stat], numstat.Count)
		case "P25":
			response.Statistics[stat] = append(response.Statistics[stat], quantiles[0])
		case "P50":
			response.Statistics[stat] = append(response.Statistics[stat], quantiles[1])
		case "P75":
			response.Statistics[stat] = append(response.Statistics[stat], quantiles[2])
		case "P90":
			response.Statistics[stat] = append(response.Statistics[stat], quantiles[3])
		case "P95":
			response.Statistics[stat] = append(response.Statistics[stat], quantiles[4])
		case "P99":
			response.Statistics[stat] = append(response.Statistics[stat], quantiles[5])
		}
	}
	return nil
}

func (tm *TreeManager) ListStatistics( // nolint: funlen
	ctx context.Context,
	database string,
	topic string,
	producer *string,
	start int,
	end int,
	bucketWidthSecs int,
) ([]AvailableStatisticsResponse, error) {
	topics := map[string]uint64{topic: 0}
	var err error
	producers := []string{}
	if producer != nil {
		producers = append(producers, *producer)
	} else {
		producers, err = tm.rootmap.Producers(ctx, database, []string{topic})
		if err != nil {
			return nil, fmt.Errorf("failed to get producers: %w", err)
		}
	}
	output := map[string]AvailableStatisticsResponse{}
	for _, producer := range producers {
		roots, err := tm.rootmap.GetLatestByTopic(ctx, database, []string{producer}, topics)
		if err != nil {
			return nil, fmt.Errorf("failed to get latest roots: %w", err)
		}
		if len(roots) != 1 {
			return nil, fmt.Errorf("expected one root, got %d", len(roots))
		}
		root := roots[0]
		if err := tree.IterateChildren(
			ctx,
			tree.NewBYOTreeReader(root.Prefix, root.NodeID, tm.ns.Get, tm.ns.GetLeafNode),
			uint64(start),
			uint64(end),
			bucketWidthSecs,
			func(child *nodestore.Child, _ uint64, _ uint64) error {
				for _, statistics := range child.Statistics {
					for i, field := range statistics.Fields {
						if _, ok := output[field.String()]; !ok {
							stats := []AvailableStatistic{}
							if _, ok := statistics.NumStats[i]; ok {
								stats = append(stats, []AvailableStatistic{
									{Name: "min", Type: "float64"},
									{Name: "max", Type: "float64"},
									{Name: "mean", Type: "float64"},
									{Name: "count", Type: "int"},
									{Name: "P25", Type: "float64"},
									{Name: "P50", Type: "float64"},
									{Name: "P75", Type: "float64"},
									{Name: "P90", Type: "float64"},
									{Name: "P95", Type: "float64"},
									{Name: "P99", Type: "float64"},
								}...)
								// apppend the numeric stats
							}
							if _, ok := statistics.TextStats[i]; ok {
								stats = append(stats, []AvailableStatistic{
									{Name: "count", Type: "int"},
									{Name: "min", Type: "string"},
									{Name: "max", Type: "string"},
								}...)
							}

							output[field.String()] = AvailableStatisticsResponse{
								Field:      field.Name,
								Type:       field.Value.String(),
								Statistics: stats,
							}
						}
					}
				}
				return nil
			},
		); err != nil {
			return nil, fmt.Errorf("failed to iterate children: %w", err)
		}
	}

	response := make([]AvailableStatisticsResponse, len(output))
	for i, k := range util.Okeys(output) {
		response[i] = output[k]
	}
	return response, nil
}

type Table struct {
	Root      rootmap.RootListing `json:"root"`
	StartTime time.Time           `json:"startTime"`
	EndTime   time.Time           `json:"endTime"`
	Children  []*nodestore.Child  `json:"children"`
}

func (tm *TreeManager) GetTables(
	ctx context.Context,
	database string,
	producer string,
	topic string,
	historical bool,
) ([]Table, error) {
	var roots []rootmap.RootListing
	var err error
	if historical {
		roots, err = tm.rootmap.GetHistorical(ctx, database, producer, topic)
		if err != nil {
			return nil, fmt.Errorf("failed to get historical roots: %w", err)
		}
	}
	if !historical {
		topics := make(map[string]uint64)
		if topic != "" {
			topics[topic] = 0
		}
		var producers []string
		if producer != "" {
			producers = []string{producer}
		}
		roots, err = tm.rootmap.GetLatestByTopic(ctx, database, producers, topics)
		if err != nil {
			return nil, fmt.Errorf("failed to get latest roots: %w", err)
		}
	}

	tables := make([]Table, 0, len(roots))
	for _, root := range roots {
		node, err := tm.ns.Get(ctx, root.Prefix, root.NodeID)
		if err != nil {
			return nil, fmt.Errorf("failed to get node: %w", err)
		}
		rootnode, ok := node.(*nodestore.InnerNode)
		if !ok {
			return nil, fmt.Errorf("unexpected node type: %w", tree.NewUnexpectedNodeError(nodestore.Inner, node))
		}
		tables = append(tables, Table{
			Root:      root,
			StartTime: time.Unix(int64(rootnode.Start), 0),
			EndTime:   time.Unix(int64(rootnode.End), 0),
			Children:  rootnode.Children,
		})
	}
	return tables, nil
}

func (tm *TreeManager) GetMessages(
	ctx context.Context,
	w io.Writer,
	start, end uint64,
	roots []rootmap.RootListing,
) error {
	if err := tm.getMessages(ctx, w, start, end, roots); err != nil {
		return fmt.Errorf("failed to get messages: %w", err)
	}
	return nil
}

func (tm *TreeManager) GetLatestRoots(
	ctx context.Context,
	database string,
	producer string,
	topics map[string]uint64,
) ([]rootmap.RootListing, error) {
	var producers []string
	if producer != "" {
		producers = []string{producer}
	}
	listing, err := tm.rootmap.GetLatestByTopic(ctx, database, producers, topics)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest roots: %w", err)
	}
	return listing, nil
}

// GetStatisticsLatest returns a summary of statistics for the given time range.
func (tm *TreeManager) GetStatisticsLatest(
	ctx context.Context,
	database,
	producer string,
	topic string,
	start, end uint64,
	granularity uint64,
) ([]nodestore.StatRange, error) {
	prefix, rootID, _, _, err := tm.rootmap.GetLatest(ctx, database, producer, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest root: %w", err)
	}
	tr := tree.NewBYOTreeReader(prefix, rootID, tm.ns.Get, tm.ns.GetLeafNode)
	ranges, err := tree.GetStatRange(ctx, tr, start, end, granularity)
	if err != nil {
		return nil, fmt.Errorf("failed to get stat range: %w", err)
	}
	return ranges, nil
}

// ForceFlush forces a synchronous flush of WAL data to the tree. Used in tests only.
func (tm *TreeManager) ForceFlush(ctx context.Context) error {
	_, err := tm.wal.ForceMerge()
	if err != nil {
		return fmt.Errorf("failed to force merge: %w", err)
	}
	if err = tm.drainWAL(ctx); err != nil {
		return fmt.Errorf("failed to drain WAL: %w", err)
	}
	return nil
}

// PrintTable returns a string representation of the tree for the given table.
func (tm *TreeManager) PrintTable(ctx context.Context, database string, producer string, topic string) string {
	prefix, root, _, _, err := tm.rootmap.GetLatest(ctx, database, producer, topic)
	if err != nil {
		return fmt.Sprintf("failed to get latest root: %v", err)
	}
	tr := tree.NewBYOTreeReader(prefix, root, tm.ns.Get, tm.ns.GetLeafNode)
	s, err := tree.Print(ctx, tr)
	if err != nil {
		return fmt.Sprintf("failed to print tree: %v", err)
	}
	return s
}

func (tm *TreeManager) newRoot(
	ctx context.Context,
	prefix string,
	start uint64,
	end uint64,
	leafWidthSecs int,
	bfactor int,
) (nodestore.NodeID, uint64, error) {
	var height uint8
	span := end - start
	coverage := uint64(leafWidthSecs)
	for coverage < span {
		coverage *= uint64(bfactor)
		height++
	}
	root := nodestore.NewInnerNode(height, start, start+coverage, bfactor)
	data := root.ToBytes()
	version, err := tm.vs.NextVersion(ctx)
	if err != nil {
		return nodestore.NodeID{}, 0, fmt.Errorf("failed to get next version: %w", err)
	}
	// build an initial data file with inner node content followed by the root
	// node ID.
	nodeID := nodestore.NewNodeID(version, 0, uint64(len(data)))
	buf := make([]byte, 0, len(data)+24)
	buf = append(buf, data...)
	buf = append(buf, nodeID[:]...)
	if err := tm.ns.Put(ctx, prefix, version, bytes.NewReader(buf)); err != nil {
		return nodestore.NodeID{}, 0, fmt.Errorf("failed to put root: %w", err)
	}
	return nodeID, version, nil
}

func (tm *TreeManager) mergeBatch(ctx context.Context, batch *wal.Batch) error {
	// Roots are created synchronously on first reception of data for a
	// topic/producer, so we should have an existing one by the time we get to
	// the point of merging.
	prefix, existingRootID, _, _, err := tm.rootmap.GetLatest(ctx, batch.Database, batch.Producer, batch.Topic)
	if err != nil && !errors.Is(err, rootmap.TableNotFoundError{}) {
		return fmt.Errorf("failed to get root: %w", err)
	}
	basereader := tree.NewBYOTreeReader(prefix, existingRootID, tm.ns.Get, tm.ns.GetLeafNode)
	readers := make([]tree.Reader, 0, len(batch.Addrs))
	for _, addr := range batch.Addrs {
		recordHeader, reader, err := tm.wal.GetReader(addr)
		if err != nil {
			return fmt.Errorf("failed to get reader: %w", err)
		}
		readers = append(readers, reader)
		for _, schema := range recordHeader.Schemas {
			if err := tm.storeSchema(ctx, batch.Database, schema); err != nil {
				return fmt.Errorf("failed to store schema: %w", err)
			}
		}
	}
	version, err := tm.vs.NextVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}
	var nodeID nodestore.NodeID
	var innerNodes []util.Pair[nodestore.NodeID, *nodestore.InnerNode]
	if err := util.RunPipe(ctx,
		func(ctx context.Context, w io.Writer) error {
			var err error
			if nodeID, innerNodes, err = tree.Merge(ctx, w, version, basereader, readers...); err != nil {
				return fmt.Errorf("failed to merge partial trees: %w", err)
			}
			return nil
		},
		func(ctx context.Context, r io.Reader) error {
			if err := tm.ns.Put(ctx, prefix, version, r); err != nil {
				return fmt.Errorf("failed to write node to storage: %w", err)
			}
			return nil
		},
	); err != nil {
		return fmt.Errorf("failed to merge partial trees: %w", err)
	}
	if err := tm.rootmap.Put(
		ctx,
		batch.Database,
		batch.Producer,
		batch.Topic,
		version,
		prefix,
		nodeID,
	); err != nil {
		return fmt.Errorf("failed to put root: %w", err)
	}
	// cache inner nodes
	for _, pair := range innerNodes {
		tm.ns.CacheInnerNode(pair.First, pair.Second)
	}
	return nil
}

func (tm *TreeManager) drainWAL(ctx context.Context) error {
	for len(tm.wal.Stats().PendingMerges) > 0 {
		select {
		case <-ctx.Done():
			return nil
		case batch, ok := <-tm.merges:
			if !ok {
				return nil
			}
			// synchronous
			if err := tm.mergeBatch(ctx, batch); err != nil {
				return fmt.Errorf("failed to merge batch %s: %w", batch, err)
			}
			if err := batch.Finish(); err != nil {
				return fmt.Errorf("failed to commit batch success: %w", err)
			}
		}
	}
	return nil
}

func (tm *TreeManager) spawnWALConsumers(ctx context.Context) {
	mergechans := make([]chan *wal.Batch, tm.syncWorkers)
	for i := 0; i < tm.syncWorkers; i++ {
		// todo we send data for each producer/topic to the same worker, because
		// the tree is only safe for one worker to write to at a time. We set a
		// big buffer here because it is possible that certain topics are much
		// more expensive to process than others, which can result in the
		// per-worker channels filling up and blocking the main dispatch. It
		// would be good to rearchitect this to eliminate that possibility with
		// a better mechanism than this. It also seems like some of the
		// computational work (e.g merging of leaves where applicable) should be
		// parallelizable even if the ultimate commits are not.
		mergechans[i] = make(chan *wal.Batch, 1000)
	}
	seed := maphash.MakeSeed()
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case batch := <-tm.merges:
				hash := maphash.Hash{}
				hash.SetSeed(seed)
				_, _ = hash.WriteString(batch.Producer + batch.Topic)
				bucket := hash.Sum64() % uint64(tm.syncWorkers)
				mergechans[bucket] <- batch
			}
		}
	}(ctx)

	for i := 0; i < tm.syncWorkers; i++ {
		mergechan := mergechans[i]
		go func(ctx context.Context) {
			ctx = log.AddTags(ctx, "sync worker", i)
			for batch := range mergechan {
				start := time.Now()
				if err := tm.mergeBatch(ctx, batch); err != nil {
					// todo - retry strategy?
					log.Errorf(ctx, "failed to merge batch %s: %v", batch, err)
					continue
				}

				if err := batch.Finish(); err != nil {
					// todo retry strategy
					log.Errorf(ctx, "failed to commit batch success to wal: %s", err)
					continue
				}
				log.Infow(ctx, "Merged partial tree",
					"size", util.HumanBytes(uint64(batch.Size)), "count", len(batch.Addrs),
					"producer", batch.Producer, "topic", batch.Topic, "elapsed", time.Since(start),
					"age", time.Since(batch.LastUpdate),
				)
			}
		}(ctx)
	}
	log.Infof(ctx, "Spawned %d WAL consumers", tm.syncWorkers)
}

func closeAll(ctx context.Context, closers ...*tree.Iterator) {
	var errs []error
	for _, closer := range closers {
		// Nil closer for iterators that had no initial message in range.
		if closer == nil {
			continue
		}
		if err := closer.Close(ctx); err != nil {
			errs = append(errs, closer.Close(ctx))
		}
	}
	for _, err := range errs {
		if err != nil {
			log.Errorf(ctx, "Failed to close iterator: %v", err)
		}
	}
}

func (tm *TreeManager) NewTreeIterator(
	ctx context.Context,
	database string,
	producer string,
	topic string,
	descending bool,
	start, end uint64,
	childFilter func(*nodestore.Child) (bool, error),
) (mcap.ContextMessageIterator, error) {
	prefix, rootID, _, truncationVersion, err := tm.rootmap.GetLatest(ctx, database, producer, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest root: %w", err)
	}
	tr := tree.NewBYOTreeReader(prefix, rootID, tm.ns.Get, tm.ns.GetLeafNode)
	return tree.NewTreeIterator(tr, descending, start, end, truncationVersion, childFilter), nil
}

func (tm *TreeManager) Truncate(
	ctx context.Context,
	database string,
	producer string,
	topic string,
	timestamp int64,
) error {
	err := tm.rootmap.Truncate(ctx, database, producer, topic, timestamp)
	if err != nil {
		return fmt.Errorf("truncation failure: %w", err)
	}
	return nil
}

func (tm *TreeManager) loadIterators(
	ctx context.Context,
	pq *util.PriorityQueue[record],
	roots []rootmap.RootListing,
	start, end uint64,
) ([]*tree.Iterator, error) {
	ch := make(chan util.Pair[int, *tree.Iterator], len(roots))
	g := errgroup.Group{}
	iterators := make([]util.Pair[int, *tree.Iterator], 0, len(roots))
	mtx := &sync.Mutex{}
	for i, root := range roots {
		g.Go(func() error {
			tr := tree.NewBYOTreeReader(root.Prefix, root.NodeID, tm.ns.Get, tm.ns.GetLeafNode)
			it := tree.NewTreeIterator(tr, false, start, end, root.MinVersion, nil)
			schema, channel, message, err := it.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					ch <- util.NewPair[int, *tree.Iterator](i, nil)
					return nil
				}
				return fmt.Errorf("failed to get next message from root %s: %w", root.NodeID, err)
			}
			mtx.Lock()
			heap.Push(pq, record{schema, channel, message, i})
			mtx.Unlock()
			ch <- util.NewPair(i, it)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed to get iterators: %w", err)
	}
	for range roots {
		iterators = append(iterators, <-ch)
	}
	sort.Slice(iterators, func(i, j int) bool {
		return iterators[i].First < iterators[j].First
	})
	return util.Map(func(p util.Pair[int, *tree.Iterator]) *tree.Iterator {
		return p.Second
	}, iterators), nil
}

var compressorPool = &sync.Pool{ // nolint:gochecknoglobals
	New: func() interface{} {
		compressor, err := mcap.NewZSTDCompressor()
		if err != nil {
			panic(fmt.Sprintf("failed to create compressor: %v", err))
		}

		return compressor
	},
}

func (tm *TreeManager) getMessages(
	ctx context.Context,
	w io.Writer,
	start, end uint64,
	roots []rootmap.RootListing,
) error {
	pq := util.NewPriorityQueue(func(a, b record) bool {
		if a.message.LogTime == b.message.LogTime {
			return a.message.ChannelID < b.message.ChannelID
		}
		return a.message.LogTime < b.message.LogTime
	})
	heap.Init(pq)
	// with one goroutine per root, construct a tree iterator and push the first
	// message onto the heap. Loading the first message will do the initial
	// traversal down to the first covered leaf and decompress the first chunk,
	// i.e it'll do some initial io.
	iterators, err := tm.loadIterators(ctx, pq, roots, start, end)
	if err != nil {
		return fmt.Errorf("failed to load iterators: %w", err)
	}
	defer closeAll(ctx, iterators...)

	writer, err := mcap.NewWriter(w)
	if err != nil {
		return fmt.Errorf("failed to create mcap writer: %w", err)
	}
	defer writer.Close()
	if err := writer.WriteHeader(&fmcap.Header{}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	mc := mcap.NewMergeCoordinator(writer)
	for pq.Len() > 0 {
		rec, ok := heap.Pop(pq).(record)
		if !ok {
			return errors.New("failed to pop record from priority queue")
		}
		if err := mc.Write(rec.schema, rec.channel, rec.message, false); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
		s, c, m, err := iterators[rec.idx].Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return fmt.Errorf("failed to get next message: %w", err)
		}
		heap.Push(pq, record{s, c, m, rec.idx})
	}
	return nil
}

// insert data into the tree and flush it to the WAL.
func (tm *TreeManager) insert(
	ctx context.Context,
	database string,
	producer string,
	topic string,
	time uint64,
	messageKeys []nodestore.MessageKey,
	stats map[string]*nodestore.Statistics,
	data []byte,
) error {
	prefix, rootID, _, _, err := tm.rootmap.GetLatest(ctx, database, producer, topic)
	if err != nil {
		return fmt.Errorf("failed to get root ID: %w", err)
	}
	version, err := tm.vs.NextVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}
	currentRoot, err := tm.ns.Get(ctx, prefix, rootID)
	if err != nil {
		return fmt.Errorf("failed to get root: %w", err)
	}
	currentRootNode, ok := currentRoot.(*nodestore.InnerNode)
	if !ok {
		return fmt.Errorf("unexpected node type: %w", tree.NewUnexpectedNodeError(nodestore.Inner, currentRoot))
	}

	mt, err := tree.NewInsert(ctx, currentRootNode, version, time, messageKeys, stats, data)
	if err != nil {
		return fmt.Errorf("insertion failure: %w", err)
	}
	// serialize the nodes with a temporary object ID of zero. When they are
	// merged into the tree this will be replaced with the new version of the
	// merge.
	serialized, err := mt.ToBytes(ctx, 0)
	if err != nil {
		return fmt.Errorf("failed to serialize tree: %w", err)
	}
	rw, err := mcap.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create mcap reader: %w", err)
	}
	info, err := rw.Info()
	if err != nil {
		return fmt.Errorf("failed to read mcap info: %w", err)
	}
	schemas := []*fmcap.Schema{}
	for _, schema := range info.Schemas {
		schemas = append(schemas, schema)
	}
	_, err = tm.wal.Insert(database, producer, topic, schemas, serialized)
	if err != nil {
		return fmt.Errorf("failed to insert into WAL: %w", err)
	}
	return nil
}

type record struct {
	schema  *fmcap.Schema
	channel *fmcap.Channel
	message *fmcap.Message
	idx     int
}

type treeDimensions struct {
	height  uint8
	bfactor int
	start   uint64
	end     uint64
}

func (td treeDimensions) bounds(ts uint64) (uint64, uint64) {
	width := td.end - td.start
	for i := 0; i < int(td.height); i++ {
		width /= uint64(td.bfactor)
	}
	inset := ts/1e9 - td.start
	bucket := inset / width
	return td.start + width*bucket, td.start + width*(bucket+1)
}

func (tm *TreeManager) dimensions(
	ctx context.Context,
	database string,
	producer string,
	topic string,
) (*treeDimensions, error) {
	prefix, root, _, _, err := tm.rootmap.GetLatest(ctx, database, producer, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest root: %w", err)
	}
	node, err := tm.ns.Get(ctx, prefix, root)
	if err != nil {
		return nil, fmt.Errorf("failed to look up node %s/%s: %w", prefix, root, err)
	}
	inner, ok := node.(*nodestore.InnerNode)
	if !ok {
		return nil, fmt.Errorf("unexpected node type: %w", tree.NewUnexpectedNodeError(nodestore.Inner, node))
	}
	return &treeDimensions{
		height:  inner.Height,
		bfactor: len(inner.Children),
		start:   inner.Start,
		end:     inner.End,
	}, nil
}
