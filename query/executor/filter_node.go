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

// FilterNode represents the filter node.
type FilterNode struct {
	child  Node
	filter func(*Tuple) (bool, error)
}

// Next returns the next tuple from the node.
func (n *FilterNode) Next(ctx context.Context) (*Tuple, error) {
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
func (n *FilterNode) Close() error {
	if err := n.child.Close(); err != nil {
		return fmt.Errorf("failed to close filter node: %w", err)
	}
	return nil
}

// String returns a string representation of the node.
func (n *FilterNode) String() string {
	return fmt.Sprintf("[filter %s]", n.child.String())
}

func NewFilterNode(filter func(*Tuple) (bool, error), child Node) *FilterNode {
	return &FilterNode{child: child, filter: filter}
}
