package treemgr

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/rootmap"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/versionstore"
)

type StatisticalSummary struct {
	Count int
}

type TreeManager interface {
	GetMessages(context.Context, uint64, uint64, []string, uint64) (io.Reader, error)
	GetStatistics(context.Context, uint64, uint64, string, uint64) (StatisticalSummary, error)
	Insert(context.Context, string, uint64, []byte) error

	GetMessagesLatest(context.Context, uint64, uint64, []string) (io.Reader, error)
	GetStatisticsLatest(context.Context, uint64, uint64, string) (StatisticalSummary, error)

	SyncWAL(context.Context) error
	StartWALSyncLoop(context.Context)
}

func (tm *treeManager) GetMessages(ctx context.Context, start, end uint64, streamIDs []string, limit uint64) (io.Reader, error) {
	return nil, nil
}

func (tm *treeManager) GetStatistics(ctx context.Context, start, end uint64, streamID string, limit uint64) (StatisticalSummary, error) {
	return StatisticalSummary{}, nil
}

func (tm *treeManager) GetMessagesLatest(ctx context.Context, start, end uint64, streamIDs []string) (io.Reader, error) {
	return nil, nil
}

func (tm *treeManager) GetStatisticsLatest(ctx context.Context, start, end uint64, streamID string) (StatisticalSummary, error) {
	return StatisticalSummary{}, nil
}

func NewTreeManager(
	ns *nodestore.Nodestore,
	vs versionstore.Versionstore,
	rm rootmap.Rootmap,
	batchsize int,
) TreeManager {
	return &treeManager{
		ns:        ns,
		vs:        vs,
		rootmap:   rm,
		batchsize: batchsize,
	}
}

type treeManager struct {
	ns      *nodestore.Nodestore
	vs      versionstore.Versionstore
	rootmap rootmap.Rootmap

	batchsize int
}

// Insert data into the tree and flush it to the WAL.
func (tm *treeManager) Insert(ctx context.Context, streamID string, time uint64, data []byte) error {
	rootID, err := tm.rootmap.GetLatest(ctx, streamID)
	if err != nil {
		return err
	}
	version, err := tm.vs.Next(ctx)
	if err != nil {
		return err
	}
	_, nodeIDs, err := tree.Insert(ctx, tm.ns, rootID, version, time, data)
	if err != nil {
		return err
	}
	if err := tm.ns.WALFlush(ctx, streamID, version, nodeIDs); err != nil {
		return fmt.Errorf("failed to flush to WAL: %w", err)
	}
	return nil
}

func (tm *treeManager) StartWALSyncLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
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
		if len(listing.Versions) < tm.batchsize {
			continue
		}
		roots := []nodestore.NodeID{}
		for _, nodeIDs := range listing.Versions {
			roots = append(roots, nodeIDs[0])
		}
		version, err := tm.vs.Next(ctx)
		if err != nil {
			return err
		}
		mergedPath, err := tm.ns.NodeMerge(ctx, version, roots)
		if err != nil {
			return err
		}
		rootID, err := tm.ns.Flush(ctx, mergedPath...)
		if err != nil {
			return err
		}
		if err := tm.rootmap.Put(ctx, listing.StreamID, version, rootID); err != nil {
			return err
		}
	}
	return nil
}
