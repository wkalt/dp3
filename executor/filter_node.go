package executor

import (
	"context"
	"fmt"
)

/*
FilterNode implements a filter operator, which filters tuples based on a
predicate supplied at construction.
*/

////////////////////////////////////////////////////////////////////////////////

// filterNode represents the filter node.
type filterNode struct {
	child  Node
	filter func(*tuple) (bool, error)
}

// Next returns the next tuple from the node.
func (n *filterNode) Next(ctx context.Context) (*tuple, error) {
	for {
		t, err := n.child.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read next message: %w", err)
		}
		ok, err := n.filter(t)
		if err != nil {
			return nil, fmt.Errorf("failed to filter message: %w", err)
		}
		if ok {
			return t, nil
		}
	}
}

// Close the node.
func (n *filterNode) Close() error {
	if err := n.child.Close(); err != nil {
		return fmt.Errorf("failed to close filter node: %w", err)
	}
	return nil
}

// String returns a string representation of the node.
func (n *filterNode) String() string {
	return fmt.Sprintf("[filter %s]", n.child.String())
}

func NewFilterNode(filter func(*tuple) (bool, error), child Node) *filterNode {
	return &filterNode{child: child, filter: filter}
}
