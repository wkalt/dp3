package executor

import (
	"context"
	"fmt"
	"io"

	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/util"
)

/*
The scan node is responsible for reading tuples out of storage, and resides at
the leaves of the execution tree.
*/

////////////////////////////////////////////////////////////////////////////////

// ScanNode represents a scan node.
type ScanNode struct {
	it mcap.ContextMessageIterator

	topic    string
	producer string
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
	util.SetContextData(ctx, "producer", n.producer)
	if err := n.it.Close(ctx); err != nil {
		return fmt.Errorf("failed to close scan node: %w", err)
	}
	return nil
}

// String returns a string representation of the node.
func (n *ScanNode) String() string {
	return fmt.Sprintf("[scan %s %s]", n.producer, n.topic)
}

// NewScanNode constructs a new scan node.
func NewScanNode(
	producer string,
	topic string,
	it mcap.ContextMessageIterator,
) *ScanNode {
	return &ScanNode{it: it, topic: topic, producer: producer}
}
