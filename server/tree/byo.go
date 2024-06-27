package tree

import (
	"context"
	"io"

	"github.com/wkalt/dp3/server/nodestore"
)

/*
A BYOTreeReader is a TreeReader that delegates to a user-provided function for
node retrieval. This is used to construct nodestore-backed tree readers.
*/

type byoTreeReader struct {
	root    nodestore.NodeID
	prefix  string
	get     func(context.Context, string, nodestore.NodeID) (nodestore.Node, error)
	getLeaf func(context.Context, string, nodestore.NodeID) (*nodestore.LeafNode, io.ReadSeekCloser, error)
}

// Root returns the root node ID.
func (t *byoTreeReader) Root() nodestore.NodeID {
	return t.root
}

// Get returns the node with the given ID.
func (t *byoTreeReader) Get(ctx context.Context, id nodestore.NodeID) (nodestore.Node, error) {
	return t.get(ctx, t.prefix, id)
}

// GetLeafNode returns the data for a leaf node.
func (t *byoTreeReader) GetLeafNode(ctx context.Context, id nodestore.NodeID) (
	*nodestore.LeafNode, io.ReadSeekCloser, error,
) {
	return t.getLeaf(ctx, t.prefix, id)
}

// NewBYOTreeReader creates a new BYOTreeReader.
func NewBYOTreeReader(
	prefix string,
	root nodestore.NodeID,
	get func(context.Context, string, nodestore.NodeID) (nodestore.Node, error),
	getLeaf func(context.Context, string, nodestore.NodeID) (
		*nodestore.LeafNode, io.ReadSeekCloser, error,
	),
) TreeReader {
	return &byoTreeReader{root, prefix, get, getLeaf}
}
