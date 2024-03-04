package treemgr

import (
	"context"
	"fmt"
	"io"

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
}

type treeManager struct {
	ns      *nodestore.Nodestore
	vs      versionstore.Versionstore
	rootmap rootmap.Rootmap
}

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
	if err := tm.ns.WALFlush(streamID, nodeIDs); err != nil {
		return fmt.Errorf("failed to flush to WAL: %w", err)
	}
	return nil
}
