package treemgr

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/log"
	"github.com/wkalt/dp3/versionstore"
	"golang.org/x/exp/maps"
)

var ErrNotImplemented = errors.New("not implemented")

type TreeManager struct {
	ns      *nodestore.Nodestore
	vs      versionstore.Versionstore
	rootmap rootmap.Rootmap

	batchsize   int
	syncWorkers int
}

func NewTreeManager(
	ns *nodestore.Nodestore,
	vs versionstore.Versionstore,
	rm rootmap.Rootmap,
	batchsize int,
) *TreeManager {
	return &TreeManager{
		ns:          ns,
		vs:          vs,
		rootmap:     rm,
		batchsize:   batchsize,
		syncWorkers: 10,
	}
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
	for {
		schema, channel, msg, err := it.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next message: %w", err)
		}
		var writer *writer
		var ok bool
		if writer, ok = writers[channel.Topic]; !ok {
			_, _, err := tm.rootmap.GetLatest(ctx, producerID, channel.Topic)
			if err != nil {
				switch {
				case errors.Is(err, rootmap.StreamNotFoundError{
					ProducerID: producerID,
					Topic:      channel.Topic,
				}):
					if err := tm.newRoot(ctx, producerID, channel.Topic); err != nil {
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
		if err := writer.WriteSchema(schema); err != nil {
			return fmt.Errorf("failed to write schema: %w", err)
		}
		if err := writer.WriteChannel(channel); err != nil {
			return fmt.Errorf("failed to write channel: %w", err)
		}
		if err := writer.WriteMessage(ctx, msg); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
	}
	for _, writer := range writers {
		if err := writer.Close(ctx); err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}
	}
	return nil
}

func (tm *TreeManager) GetMessages(
	ctx context.Context, start, end uint64, producerID string, topics []string, limit uint64) (io.Reader, error) {
	return nil, ErrNotImplemented
}

type StatisticalSummary struct {
	Count int
}

func (tm *TreeManager) GetStatistics(
	ctx context.Context, start, end uint64, producerID string,
	topic string, version uint64) (StatisticalSummary, error) {
	return StatisticalSummary{}, ErrNotImplemented
}

func (tm *TreeManager) GetMessagesLatest(
	ctx context.Context, w io.Writer,
	start, end uint64,
	producerID string, topics []string) error {
	pq := util.NewPriorityQueue[record, uint64]()
	heap.Init(pq)
	iterators := make([]*tree.Iterator, 0, len(topics))
	for _, topic := range topics {
		root, _, err := tm.rootmap.GetLatest(ctx, producerID, topic)
		if err != nil {
			return fmt.Errorf("failed to get latest root for %s/%s: %w", producerID, topic, err)
		}
		it, err := tree.NewTreeIterator(ctx, tm.ns, root, start, end)
		if err != nil {
			return fmt.Errorf("failed to create iterator for %s/%s: %w", producerID, topic, err)
		}
		iterators = append(iterators, it)
	}

	// pop one message from each iterator and push it onto the priority queue
	for i, it := range iterators {
		if it.More() {
			schema, channel, message, err := it.Next(ctx)
			if err != nil {
				return fmt.Errorf("failed to get next message: %w", err)
			}
			item := util.Item[record, uint64]{
				Value:    record{schema, channel, message, i},
				Priority: message.LogTime,
			}
			heap.Push(pq, &item)
		}
	}
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

func (tm *TreeManager) GetStatisticsLatest(
	ctx context.Context, start, end uint64,
	producerID string, topic string) (StatisticalSummary, error) {
	return StatisticalSummary{}, ErrNotImplemented
}

// Insert data into the tree and flush it to the WAL.
func (tm *TreeManager) Insert(
	ctx context.Context,
	producerID string,
	topic string,
	time uint64,
	data []byte,
	statistics *nodestore.Statistics,
) error {
	rootID, _, err := tm.rootmap.GetLatest(ctx, producerID, topic)
	if err != nil {
		return fmt.Errorf("failed to get root ID: %w", err)
	}
	version, err := tm.vs.Next(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}
	_, nodeIDs, err := tree.Insert(ctx, tm.ns, rootID, version, time, data, statistics)
	if err != nil {
		return fmt.Errorf("insertion failure: %w", err)
	}
	if err := tm.ns.WALFlush(ctx, producerID, topic, version, nodeIDs); err != nil {
		return fmt.Errorf("failed to flush to WAL: %w", err)
	}
	return nil
}

func (tm *TreeManager) StartWALSyncLoop(ctx context.Context) {
	ticker := time.NewTicker(120 * time.Second)
	for {
		select {
		case <-ticker.C:
			log.Infof(ctx, "syncing WAL")
			if err := tm.SyncWAL(ctx); err != nil {
				log.Errorw(ctx, "failed to sync WAL", "error", err.Error())
			}
		case <-ctx.Done():
			return
		}
	}
}

func (tm *TreeManager) SyncWAL(ctx context.Context) error {
	listings, err := tm.ns.ListWAL(ctx)
	if err != nil {
		return fmt.Errorf("failed to list WAL: %w", err)
	}
	for _, listing := range listings {
		rootID, _, err := tm.rootmap.GetLatest(ctx, listing.ProducerID, listing.Topic)
		if err != nil {
			return fmt.Errorf("failed to get latest root: %w", err)
		}
		roots := []nodestore.NodeID{}
		version, err := tm.vs.Next(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next version: %w", err)
		}
		var newRootID nodestore.NodeID
		versions := maps.Keys(listing.Versions)
		slices.Sort(versions)
		if len(versions) == 1 {
			log.Infow(ctx, "flushing WAL entries",
				"producerID", listing.ProducerID,
				"topic", listing.Topic,
				"count", len(listing.Versions),
				"version", version,
			)
			value := maps.Values(listing.Versions)[0]
			newRootID, err = tm.ns.FlushWALPath(ctx, version, value)
			if err != nil {
				return fmt.Errorf("failed to flush wal path: %w", err)
			}
		} else {
			log.Infow(ctx, "merging WAL entries",
				"producerID", listing.ProducerID,
				"topic", listing.Topic,
				"count", len(listing.Versions))
			for _, version := range versions {
				nodeIDs := listing.Versions[version]
				roots = append(roots, nodeIDs[0])
			}
			newRootID, err = tm.ns.WALMerge(ctx, rootID, version, roots)
			if err != nil {
				return fmt.Errorf("failed to merge WAL into tree: %w", err)
			}
		}
		for _, version := range versions {
			nodeIDs := listing.Versions[version]
			for _, nodeID := range nodeIDs {
				if err = tm.ns.WALDelete(ctx, nodeID); err != nil { // todo transaction
					return fmt.Errorf("failed to delete node %s from WAL: %w", nodeID, err)
				}
			}
		}
		if err := tm.rootmap.Put(ctx, listing.ProducerID, listing.Topic, version, newRootID); err != nil {
			return fmt.Errorf("failed to update rootmap: %w", err)
		}
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
	depth   uint8
	bfactor int
	start   uint64
	end     uint64
}

func (td treeDimensions) bounds(ts uint64) (uint64, uint64) {
	width := td.end - td.start
	for i := 0; i < int(td.depth); i++ {
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
		return nil, fmt.Errorf("failed to look up node: %w", err)
	}
	inner := node.(*nodestore.InnerNode)
	return &treeDimensions{
		depth:   inner.Depth,
		bfactor: len(inner.Children),
		start:   inner.Start,
		end:     inner.End,
	}, nil
}

func (tm *TreeManager) newRoot(ctx context.Context, producerID string, topic string) error {
	rootID, err := tm.ns.NewRoot(ctx, util.DateSeconds("1970-01-01"), util.DateSeconds("2038-01-19"), 60, 64)
	if err != nil {
		return fmt.Errorf("failed to create new root: %w", err)
	}
	version, err := tm.vs.Next(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}
	if err := tm.rootmap.Put(ctx, producerID, topic, version, rootID); err != nil {
		return fmt.Errorf("failed to update rootmap: %w", err)
	}
	return nil
}

func (tm *TreeManager) PrintStream(ctx context.Context, producerID string, topic string) string {
	root, version, err := tm.rootmap.GetLatest(ctx, producerID, topic)
	if err != nil {
		return fmt.Sprintf("failed to get latest root: %v", err)
	}
	s, err := tm.ns.Print(ctx, root, version, nil)
	if err != nil {
		return fmt.Sprintf("failed to print tree: %v", err)
	}
	return s
}
