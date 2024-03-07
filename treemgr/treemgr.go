package treemgr

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/versionstore"
	"golang.org/x/exp/maps"
)

type StatisticalSummary struct {
	Count int
}

type TreeManager interface {
	GetMessages(
		ctx context.Context,
		start uint64,
		end uint64,
		streamIDs []string,
		version uint64,
	) (io.Reader, error)
	GetStatistics(
		ctx context.Context,
		start uint64,
		end uint64,
		streamID string,
		version uint64,
	) (StatisticalSummary, error)
	Insert(
		ctx context.Context,
		streamID string,
		time uint64,
		data []byte,
	) error
	GetMessagesLatest(
		ctx context.Context,
		output io.Writer,
		start uint64,
		end uint64,
		streamIDs []string,
	) error
	GetStatisticsLatest(
		ctx context.Context,
		start uint64,
		end uint64,
		streamID string,
	) (StatisticalSummary, error)
	SyncWAL(ctx context.Context) error
	StartWALSyncLoop(ctx context.Context)
	IngestStream(ctx context.Context, hashid string, data io.Reader) error
}

func (tm *treeManager) GetMessages(
	ctx context.Context, start, end uint64, streamIDs []string, limit uint64) (io.Reader, error) {
	return nil, nil
}

func (tm *treeManager) GetStatistics(
	ctx context.Context, start, end uint64, streamID string, limit uint64) (StatisticalSummary, error) {
	return StatisticalSummary{}, nil
}

type record struct {
	schema  *fmcap.Schema
	channel *fmcap.Channel
	message *fmcap.Message
	idx     int
}

func (tm *treeManager) GetMessagesLatest(
	ctx context.Context, w io.Writer, start, end uint64, streamIDs []string) error {
	pq := util.NewPriorityQueue[record, uint64]()
	heap.Init(pq)
	iterators := make([]*tree.Iterator, 0, len(streamIDs))
	for _, streamID := range streamIDs {
		root, err := tm.rootmap.GetLatest(ctx, streamID)
		if err != nil {
			return fmt.Errorf("failed to get latest root for %s: %w", streamID, err)
		}
		it, err := tree.NewTreeIterator(ctx, tm.ns, root, start, end)
		if err != nil {
			return fmt.Errorf("failed to create iterator for %s: %w", streamID, err)
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

func (tm *treeManager) GetStatisticsLatest(
	ctx context.Context, start, end uint64, streamID string) (StatisticalSummary, error) {
	return StatisticalSummary{}, nil
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

func (tm *treeManager) dimensions(
	ctx context.Context,
	streamID string,
) (*treeDimensions, error) {
	root, err := tm.rootmap.GetLatest(ctx, streamID)
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

func NewTreeManager(
	ns *nodestore.Nodestore,
	vs versionstore.Versionstore,
	rm rootmap.Rootmap,
	batchsize int,
) TreeManager {
	return &treeManager{
		ns:          ns,
		vs:          vs,
		rootmap:     rm,
		batchsize:   batchsize,
		syncWorkers: 10,
	}
}

type treeManager struct {
	ns      *nodestore.Nodestore
	vs      versionstore.Versionstore
	rootmap rootmap.Rootmap

	batchsize   int
	syncWorkers int
}

func (tm *treeManager) newRoot(ctx context.Context, streamID string) error {
	rootID, err := tm.ns.NewRoot(ctx, util.DateSeconds("1970-01-01"), util.DateSeconds("2038-01-19"), 60, 64)
	if err != nil {
		return fmt.Errorf("failed to create new root: %w", err)
	}
	version, err := tm.vs.Next(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}
	if err := tm.rootmap.Put(ctx, streamID, version, rootID); err != nil {
		return fmt.Errorf("failed to update rootmap: %w", err)
	}
	return nil
}

func (tm *treeManager) IngestStream(ctx context.Context, hashid string, data io.Reader) error {
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
			streamID := util.ComputeStreamID(hashid, channel.Topic)
			_, err := tm.rootmap.GetLatest(ctx, streamID)
			if err != nil {
				switch {
				case errors.Is(err, nodestore.ErrNodeNotFound):
					if err := tm.newRoot(ctx, streamID); err != nil {
						return fmt.Errorf("failed to create new root: %w", err)
					}
				default:
					return fmt.Errorf("failed to get latest root: %w", err)
				}
			}
			writer, err = newWriter(ctx, tm, streamID)
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

// Insert data into the tree and flush it to the WAL.
func (tm *treeManager) Insert(ctx context.Context, streamID string, time uint64, data []byte) error {
	rootID, err := tm.rootmap.GetLatest(ctx, streamID)
	if err != nil {
		return fmt.Errorf("failed to get root ID: %w", err)
	}
	version, err := tm.vs.Next(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}
	_, nodeIDs, err := tree.Insert(ctx, tm.ns, rootID, version, time, data)
	if err != nil {
		return fmt.Errorf("insertion failure: %w", err)
	}
	if err := tm.ns.WALFlush(ctx, streamID, version, nodeIDs); err != nil {
		return fmt.Errorf("failed to flush to WAL: %w", err)
	}
	return nil
}

func (tm *treeManager) StartWALSyncLoop(ctx context.Context) {
	ticker := time.NewTicker(120 * time.Second)
	for {
		select {
		case <-ticker.C:
			slog.InfoContext(ctx, "syncing WAL")
			if err := tm.SyncWAL(ctx); err != nil {
				slog.ErrorContext(ctx, "failed to sync WAL: "+err.Error())
			}
		case <-ctx.Done():
			return
		}
	}
}

func (tm *treeManager) SyncWAL(ctx context.Context) error {
	listings, err := tm.ns.ListWAL(ctx)
	if err != nil {
		return fmt.Errorf("failed to list WAL: %w", err)
	}
	for _, listing := range listings {
		roots := []nodestore.NodeID{}
		version, err := tm.vs.Next(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next version: %w", err)
		}
		var rootID nodestore.NodeID
		if len(listing.Versions) == 1 {
			slog.InfoContext(ctx, "flushing WAL entries", "streamID", listing.StreamID, "count", len(listing.Versions))
			value := maps.Values(listing.Versions)[0]
			rootID, err = tm.ns.FlushWALPath(ctx, value)
			if err != nil {
				return fmt.Errorf("failed to flush wal path: %w", err)
			}
		} else {
			slog.InfoContext(ctx, "merging WAL entries", "streamID", listing.StreamID, "count", len(listing.Versions))
			for _, nodeIDs := range listing.Versions {
				roots = append(roots, nodeIDs[0])
			}
			rootID, err = tm.ns.WALMerge(ctx, version, roots)
			if err != nil {
				return fmt.Errorf("failed to merge WAL into tree: %w", err)
			}
		}
		for _, nodeIDs := range listing.Versions {
			for _, nodeID := range nodeIDs {
				if err = tm.ns.WALDelete(ctx, nodeID); err != nil { // todo transaction
					return fmt.Errorf("failed to delete node %s from WAL: %w", nodeID, err)
				}
			}
		}
		if err := tm.rootmap.Put(ctx, listing.StreamID, version, rootID); err != nil {
			return fmt.Errorf("failed to update rootmap: %w", err)
		}
	}
	return nil
}
