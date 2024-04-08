package executor

import (
	"context"
	"fmt"
	"io"

	"github.com/wkalt/dp3/tree"
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
		Schema:  s,
		Channel: c,
		Message: m,
	}, nil
}

// Close the node.
func (n *ScanNode) Close() error {
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
