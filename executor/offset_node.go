package executor

import (
	"context"
	"fmt"
)

/*
OffsetNode implements the usual offset operator.
*/

////////////////////////////////////////////////////////////////////////////////

// offsetNode represents the offset node.
type offsetNode struct {
	child  Node
	offset int
}

// NewOffsetNode constructs a new offset node.
func NewOffsetNode(offset int, child Node) *offsetNode {
	return &offsetNode{offset: offset, child: child}
}

// Next returns the next tuple from the node.
func (n *offsetNode) Next(ctx context.Context) (*tuple, error) {
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
func (n *offsetNode) Close() error {
	if err := n.child.Close(); err != nil {
		return fmt.Errorf("failed to close offset node: %w", err)
	}
	return nil
}

// String returns a string representation of the node.
func (n *offsetNode) String() string {
	return fmt.Sprintf("[offset %d %s]", n.offset, n.child.String())
}
