package tree

import (
	"context"
	"io"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
)

/*
A BYOTreeReader is a TreeReader that delegates to a user-provided function for
node retrieval. This is used to construct nodestore-backed tree readers.
*/

type byoTreeReader struct {
	root   nodestore.NodeID
	prefix string
	get    func(context.Context, string, nodestore.NodeID) (nodestore.Node, error)
}

// Root returns the root node ID.
func (t *byoTreeReader) Root() nodestore.NodeID {
	return t.root
}

// Get returns the node with the given ID.
func (t *byoTreeReader) Get(ctx context.Context, id nodestore.NodeID) (nodestore.Node, error) {
	return t.get(ctx, t.prefix, id)
}

// GetLeafData returns the data for a leaf node.
func (t *byoTreeReader) GetLeafData(ctx context.Context, id nodestore.NodeID) (
	nodestore.NodeID, io.ReadSeekCloser, error,
) {
	var ancestor nodestore.NodeID
	node, err := t.get(ctx, t.prefix, id)
	if err != nil {
		return ancestor, nil, err
	}

	leaf, ok := node.(*nodestore.LeafNode)
	if !ok {
		return ancestor, nil, NewUnexpectedNodeError(nodestore.Leaf, node)
	}
	return leaf.Ancestor(), util.NewReadSeekNopCloser(leaf.Data()), nil
}

// NewBYOTreeReader creates a new BYOTreeReader.
func NewBYOTreeReader(
	prefix string,
	root nodestore.NodeID,
	get func(context.Context, string, nodestore.NodeID) (nodestore.Node, error),
) TreeReader {
	return &byoTreeReader{root, prefix, get}
}
