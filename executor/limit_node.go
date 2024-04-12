package executor

import (
	"context"
	"fmt"
	"io"
)

/*
LimitNode implements the usual limit operator.
*/

////////////////////////////////////////////////////////////////////////////////

// limitNode represents the limit node.
type limitNode struct {
	limit int
	child Node
}

// NewLimitNode constructs a new limit node.
func NewLimitNode(limit int, child Node) *limitNode {
	return &limitNode{limit: limit, child: child}
}

// Next returns the next tuple from the node.
func (n *limitNode) Next(ctx context.Context) (*tuple, error) {
	if n.limit == 0 {
		return nil, io.EOF
	}
	n.limit--
	next, err := n.child.Next(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read next message: %w", err)
	}
	return next, nil
}

// Close the node.
func (n *limitNode) Close() error {
	if err := n.child.Close(); err != nil {
		return fmt.Errorf("failed to close limit node: %w", err)
	}
	return nil
}

// String returns a string representation of the node.
func (n *limitNode) String() string {
	return fmt.Sprintf("[limit %d %s]", n.limit, n.child.String())
}
