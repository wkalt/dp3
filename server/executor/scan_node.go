package executor

import (
	"context"
	"fmt"
	"io"

	"github.com/wkalt/dp3/server/tree"
	"github.com/wkalt/dp3/server/util"
)

/*
The scan node is responsible for reading tuples out of storage, and resides at
the leaves of the execution tree.
*/

////////////////////////////////////////////////////////////////////////////////

// ScanNode represents a scan node.
type ScanNode struct {
	it *tree.Iterator

	topic string
}

// Next returns the next tuple from the node.
func (n *ScanNode) Next(ctx context.Context) (*Tuple, error) {
	if !n.it.More() {
		return nil, io.EOF
	}
	s, c, m, err := n.it.Next(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to scan next message: %w", err)
	}
	return &Tuple{
		schema:  s,
		channel: c,
		message: m,
	}, nil
}

// Close the node.
func (n *ScanNode) Close(ctx context.Context) error {
	util.SetContextData(ctx, "topic", n.topic)

	stats := n.it.Stats()
	util.SetContextValue(ctx, "inner_nodes_filtered", float64(stats.InnerNodesFiltered))
	util.SetContextValue(ctx, "inner_nodes_scanned", float64(stats.InnerNodesScanned))
	util.SetContextValue(ctx, "leaf_nodes_filtered", float64(stats.LeafNodesFiltered))
	util.SetContextValue(ctx, "leaf_nodes_scanned", float64(stats.LeafNodesScanned))

	if err := n.it.Close(); err != nil {
		return fmt.Errorf("failed to close scan node: %w", err)
	}
	return nil
}

// String returns a string representation of the node.
func (n *ScanNode) String() string {
	return fmt.Sprintf("[scan %s]", n.topic)
}

// NewScanNode constructs a new scan node.
func NewScanNode(
	topic string,
	it *tree.Iterator,
) *ScanNode {
	return &ScanNode{it: it, topic: topic}
}
