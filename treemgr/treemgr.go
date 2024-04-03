package treemgr

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"sort"
	"sync"
	"time"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/log"
	"github.com/wkalt/dp3/versionstore"
	"github.com/wkalt/dp3/wal"
	"golang.org/x/exp/maps"
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
	vs      versionstore.Versionstore
	rootmap rootmap.Rootmap
	merges  <-chan *wal.Batch

	syncWorkers int

	wal *wal.WALManager
}

// NewRoot creates a new root for the provided producer and topic.
func (tm *TreeManager) NewRoot(ctx context.Context, producer string, topic string) error {
	rootID, version, err := tm.newRoot(
		ctx,
		util.DateSeconds("1970-01-01"),
		util.DateSeconds("2038-01-19"),
		60,
		64,
	)
	if err != nil {
		return fmt.Errorf("failed to create new root: %w", err)
	}
	// todo two inserts can race here and get a unique violation. need to handle that.
	if err := tm.rootmap.Put(ctx, producer, topic, version, rootID); err != nil {
		return fmt.Errorf("failed to put root: %w", err)
	}
	return nil
}

// NewTreeManager returns a new TreeManager.
func NewTreeManager(
	ctx context.Context,
	ns *nodestore.Nodestore,
	vs versionstore.Versionstore,
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

// Receive an MCAP data stream on behalf of a particular producer. The data is
// split by topic and hashed with the producer ID, to result in one tree in
// storage per topic per producer. After Receive returns, the data is in the
// WAL, not in final storage. To get to final storage, a call to SyncWAL must
// occur, which performs a merge of pending partial trees in the WAL and writes
// a single object to storage, in the interest of making write sizes independent
// of the input data sizes.
func (tm *TreeManager) Receive(ctx context.Context, producerID string, data io.Reader) error {
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
	for {
		schema, channel, msg, err := it.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next message: %w", err)
		}
		if len(msg.Data) > len(buf) {
			buf = make([]byte, 2*len(msg.Data))
		}
		var writer *writer
		var ok bool
		if writer, ok = writers[channel.Topic]; !ok {
			if _, _, err := tm.rootmap.GetLatest(ctx, producerID, channel.Topic); err != nil {
				switch {
				case errors.Is(err, rootmap.StreamNotFoundError{}):
					if err := tm.NewRoot(ctx, producerID, channel.Topic); err != nil {
						return fmt.Errorf("failed to create new root: %w", err)
					}
				default:
					return fmt.Errorf("failed to get latest root: %w", err)
				}
			}
			writer, err = newWriter(ctx, tm, producerID, channel.Topic)
			if err != nil {
				return fmt.Errorf("failed to create writer: %w", err)
			}
			writers[channel.Topic] = writer
		}
		if err := writer.Write(ctx, schema, channel, msg); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
	}

	keys := maps.Keys(writers)
	sort.Strings(keys)
	for _, k := range keys {
		writer := writers[k]
		if err := writer.Close(ctx); err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}
	}
	return nil
}

// StatisticalSummary is a summary of statistics for a given time range.
type StatisticalSummary struct {
	Start      uint64                `json:"start"`
	End        uint64                `json:"end"`
	Statistics *nodestore.Statistics `json:"statistics"`
}

// GetStatistics returns a summary of statistics for the given time range.
func (tm *TreeManager) GetStatistics(
	ctx context.Context,
	start, end uint64,
	producerID string,
	topic string,
	version uint64,
	granularity uint64,
) ([]nodestore.StatRange, error) {
	rootID, err := tm.rootmap.Get(ctx, producerID, topic, version)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest root: %w", err)
	}
	tr := tree.NewBYOTreeReader(rootID, tm.ns.Get)
	ranges, err := tree.GetStatRange(ctx, tr, start, end, granularity)
	if err != nil {
		return nil, fmt.Errorf("failed to get stat range: %w", err)
	}
	return ranges, nil
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
	producerID string,
	topics map[string]uint64,
) ([]rootmap.RootListing, error) {
	listing, err := tm.rootmap.GetLatestByTopic(ctx, producerID, topics)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest roots: %w", err)
	}
	return listing, nil
}

// GetStatisticsLatest returns a summary of statistics for the given time range.
func (tm *TreeManager) GetStatisticsLatest(
	ctx context.Context,
	start, end uint64,
	producerID string,
	topic string,
	granularity uint64,
) ([]nodestore.StatRange, error) {
	rootID, _, err := tm.rootmap.GetLatest(ctx, producerID, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest root: %w", err)
	}
	tr := tree.NewBYOTreeReader(rootID, tm.ns.Get)
	ranges, err := tree.GetStatRange(ctx, tr, start, end, granularity)
	if err != nil {
		return nil, fmt.Errorf("failed to get stat range: %w", err)
	}
	return ranges, nil
}

// ForceFlush forces a synchronous flush of WAL data to the tree. Used in tests only.
func (tm *TreeManager) ForceFlush(ctx context.Context) error {
	c, err := tm.wal.ForceMerge(ctx)
	if err != nil {
		return fmt.Errorf("failed to force merge: %w", err)
	}
	err = tm.drainWAL(ctx, c)
	if err != nil {
		return fmt.Errorf("failed to drain WAL: %w", err)
	}
	return nil
}

// PrintStream returns a string representation of the tree for the given stream.
func (tm *TreeManager) PrintStream(ctx context.Context, producerID string, topic string) string {
	root, _, err := tm.rootmap.GetLatest(ctx, producerID, topic)
	if err != nil {
		return fmt.Sprintf("failed to get latest root: %v", err)
	}
	tr := tree.NewBYOTreeReader(root, tm.ns.Get)
	s, err := tree.Print(ctx, tr)
	if err != nil {
		return fmt.Sprintf("failed to print tree: %v", err)
	}
	return s
}

func (tm *TreeManager) newRoot(
	ctx context.Context,
	start uint64,
	end uint64,
	leafWidthSecs int,
	bfactor int,
) (nodestore.NodeID, uint64, error) {
	var height int
	span := end - start
	coverage := leafWidthSecs
	for uint64(coverage) < span {
		coverage *= bfactor
		height++
	}
	root := nodestore.NewInnerNode(uint8(height), start, start+uint64(coverage), bfactor)
	data := root.ToBytes()
	version, err := tm.vs.Next(ctx)
	if err != nil {
		return nodestore.NodeID{}, 0, fmt.Errorf("failed to get next version: %w", err)
	}
	if err := tm.ns.Put(ctx, version, data); err != nil {
		return nodestore.NodeID{}, 0, fmt.Errorf("failed to put root: %w", err)
	}
	nodeID := nodestore.NewNodeID(version, 0, uint64(len(data)))
	return nodeID, version, nil
}

func (tm *TreeManager) mergeBatch(ctx context.Context, batch *wal.Batch) error {
	trees := make([]tree.TreeReader, 0, len(batch.Addrs)+1)

	// there should be an existing root
	// If there is an existing root, then we need to add that data to the merge.
	existingRootID, _, err := tm.rootmap.GetLatest(ctx, batch.ProducerID, batch.Topic)
	if err != nil && !errors.Is(err, rootmap.StreamNotFoundError{}) {
		return fmt.Errorf("failed to get root: %w", err)
	}
	basereader := tree.NewBYOTreeReader(existingRootID, tm.ns.Get)

	for _, addr := range batch.Addrs {
		page, err := tm.wal.Get(addr)
		if err != nil {
			return fmt.Errorf("failed to get page %s: %w", addr, err)
		}
		var mt tree.MemTree
		if err := mt.FromBytes(ctx, page); err != nil {
			return fmt.Errorf("failed to deserialize tree: %w", err)
		}
		trees = append(trees, &mt)
	}
	version, err := tm.vs.Next(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}

	var merged tree.MemTree
	if err := tree.Merge(ctx, &merged, basereader, trees...); err != nil {
		return fmt.Errorf("failed to merge partial trees: %w", err)
	}
	data, err := merged.ToBytes(ctx, version)
	if err != nil {
		return fmt.Errorf("failed to serialize partial tree: %w", err)
	}
	rootID := data[len(data)-24:]
	if err := tm.ns.Put(ctx, version, data[:len(data)-24]); err != nil {
		return fmt.Errorf("failed to put tree: %w", err)
	}
	if err := tm.rootmap.Put(ctx, batch.ProducerID, batch.Topic, version, nodestore.NodeID(rootID)); err != nil {
		return fmt.Errorf("failed to put root: %w", err)
	}
	return nil
}

func (tm *TreeManager) drainWAL(ctx context.Context, n int) error {
	c := 0
	for c < n {
		select {
		case <-ctx.Done():
			return nil
		case batch, ok := <-tm.merges:
			if !ok {
				return nil
			}
			if err := tm.mergeBatch(ctx, batch); err != nil {
				return fmt.Errorf("failed to merge batch %s: %w", batch.ID, err)
			}
			if err := batch.Finish(); err != nil {
				return fmt.Errorf("failed to commit batch success: %w", err)
			}
		}
		c++
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
				_, _ = hash.WriteString(batch.ProducerID + batch.Topic)
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
					log.Errorf(ctx, "failed to merge batch %s: %v", batch.ID, err)
					continue
				}

				if err := batch.Finish(); err != nil {
					// todo retry strategy
					log.Errorf(ctx, "failed to commit batch success to wal: %s", err)
					continue
				}
				log.Infow(ctx, "Merged partial tree",
					"size", util.HumanBytes(uint64(batch.Size)), "count", len(batch.Addrs),
					"producer", batch.ProducerID, "topic", batch.Topic, "elapsed", time.Since(start),
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
		if err := closer.Close(); err != nil {
			errs = append(errs, closer.Close())
		}
	}
	for _, err := range errs {
		if err != nil {
			log.Errorf(ctx, "Failed to close iterator: %v", err)
		}
	}
}

func (tm *TreeManager) loadIterators(
	ctx context.Context,
	pq *util.PriorityQueue[record, uint64],
	roots []rootmap.RootListing,
	start, end uint64,
) ([]*tree.Iterator, error) {
	ch := make(chan *tree.Iterator, len(roots))
	g := errgroup.Group{}
	iterators := make([]*tree.Iterator, 0, len(roots))
	mtx := &sync.Mutex{}
	for i, root := range roots {
		g.Go(func() error {
			tr := tree.NewBYOTreeReader(root.NodeID, tm.ns.Get)
			it := tree.NewTreeIterator(ctx, tr, start, end, root.RequestedMinVersion)
			schema, channel, message, err := it.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					ch <- nil
					return nil
				}
				return fmt.Errorf("failed to get next message: %w", err)
			}
			item := util.Item[record, uint64]{
				Value:    record{schema, channel, message, i},
				Priority: message.LogTime,
			}
			mtx.Lock()
			heap.Push(pq, &item)
			mtx.Unlock()
			ch <- it
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed to get iterators: %w", err)
	}
	for range roots {
		if it := <-ch; it != nil {
			iterators = append(iterators, it)
		}
	}
	return iterators, nil
}

func (tm *TreeManager) getMessages(
	ctx context.Context,
	w io.Writer,
	start, end uint64,
	roots []rootmap.RootListing,
) error {
	pq := util.NewPriorityQueue[record, uint64]()
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
		rec := heap.Pop(pq).(record)
		if err := mc.Write(rec.schema, rec.channel, rec.message); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
		s, c, m, err := iterators[rec.idx].Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return fmt.Errorf("failed to get next message: %w", err)
		}
		var item = util.Item[record, uint64]{
			Value:    record{s, c, m, rec.idx},
			Priority: m.LogTime,
		}
		heap.Push(pq, &item)
	}
	return nil
}

// insert data into the tree and flush it to the WAL.
func (tm *TreeManager) insert(
	ctx context.Context,
	producerID string,
	topic string,
	time uint64,
	data []byte,
	statistics map[string]*nodestore.Statistics,
) error {
	rootID, _, err := tm.rootmap.GetLatest(ctx, producerID, topic)
	if err != nil {
		return fmt.Errorf("failed to get root ID: %w", err)
	}
	version, err := tm.vs.Next(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}
	currentRoot, err := tm.ns.Get(ctx, rootID)
	if err != nil {
		return fmt.Errorf("failed to get root: %w", err)
	}
	mt := tree.NewMemTree(rootID, currentRoot.(*nodestore.InnerNode))
	if err := tree.Insert(ctx, mt, version, time, data, statistics); err != nil {
		return fmt.Errorf("insertion failure: %w", err)
	}
	serialized, err := mt.ToBytes(ctx, 0) // zero is a temporary oid
	if err != nil {
		return fmt.Errorf("failed to serialize tree: %w", err)
	}
	_, err = tm.wal.Insert(ctx, producerID, topic, serialized)
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
	producerID string,
	topic string,
) (*treeDimensions, error) {
	root, _, err := tm.rootmap.GetLatest(ctx, producerID, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest root: %w", err)
	}
	node, err := tm.ns.Get(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("failed to look up node %s: %w", root, err)
	}
	inner := node.(*nodestore.InnerNode)
	return &treeDimensions{
		height:  inner.Height,
		bfactor: len(inner.Children),
		start:   inner.Start,
		end:     inner.End,
	}, nil
}
