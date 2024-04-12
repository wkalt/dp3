package executor

import (
	"context"
	"fmt"
)

/*
OffsetNode implements the usual offset operator.
*/

////////////////////////////////////////////////////////////////////////////////

// OffsetNode represents the offset node.
type OffsetNode struct {
	child  Node
	offset int
}

// NewOffsetNode constructs a new offset node.
func NewOffsetNode(offset int, child Node) *OffsetNode {
	return &OffsetNode{offset: offset, child: child}
}

// Next returns the next tuple from the node.
func (n *OffsetNode) Next(ctx context.Context) (*Tuple, error) {
	for n.offset > 0 {
		_, err := n.child.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read next message: %w", err)
		}
		n.offset--
	}
	next, err := n.child.Next(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read next message: %w", err)
	}
	return next, nil
}

// Close the node.
func (n *OffsetNode) Close() error {
	if err := n.child.Close(); err != nil {
		return fmt.Errorf("failed to close offset node: %w", err)
	}
	return nil
}

// String returns a string representation of the node.
func (n *OffsetNode) String() string {
	return fmt.Sprintf("[offset %d %s]", n.offset, n.child.String())
}
