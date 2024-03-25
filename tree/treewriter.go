package tree

import (
	"context"

	"github.com/wkalt/dp3/nodestore"
)

/*
The TreeWriter interface is used to write nodes to a tree. It extends the
TreeReader interface with methods to write nodes and set the tree root.
*/

type TreeWriter interface {
	Put(ctx context.Context, id nodestore.NodeID, node nodestore.Node) error
	SetRoot(id nodestore.NodeID)
	TreeReader
}