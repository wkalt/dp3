package executor

import (
	"context"
	"io"

	fmcap "github.com/foxglove/mcap/go/mcap"
)

/*
MockNode is a mock implementation of a node, used to simulate scan nodes in
tests, without a storage dependency.
*/

////////////////////////////////////////////////////////////////////////////////

// mockNode is a mock implementation of a node, used to simulate scan nodes in
// tests.
type mockNode struct {
	tuples []*tuple
}

// Next returns the next tuple from the node.
func (n *mockNode) Next(ctx context.Context) (*tuple, error) {
	if len(n.tuples) == 0 {
		return nil, io.EOF
	}
	t := n.tuples[0]
	n.tuples = n.tuples[1:]
	return t, nil
}

// String returns a string representation of the node.
func (n *mockNode) String() string {
	return "[mock]"
}

// Close the node.
func (n *mockNode) Close() error {
	return nil
}

// NewMockNode constructs a new mock node.
func NewMockNode(stamps ...uint64) Node {
	tuples := make([]*tuple, 0, len(stamps))
	for _, stamp := range stamps {
		tuples = append(tuples, newTuple(nil, nil, &fmcap.Message{
			LogTime: stamp,
		}))
	}
	return &mockNode{tuples: tuples}
}
