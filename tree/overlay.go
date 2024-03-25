package tree

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
)

type Overlay struct {
	Trees []TreeReader
}

func (o *Overlay) Root() nodestore.NodeID {
	return o.Trees[len(o.Trees)-1].Root()
}

func (o *Overlay) Get(ctx context.Context, id nodestore.NodeID) (nodestore.Node, error) {
	for i := len(o.Trees) - 1; i >= 0; i-- {
		node, err := o.Trees[i].Get(ctx, id)
		if err != nil {
			if errors.Is(err, nodestore.NodeNotFoundError{}) {
				continue
			}
			return nil, fmt.Errorf("failed to get node: %w", err)
		}
		return node, nil
	}
	return nil, nodestore.NodeNotFoundError{NodeID: id}
}

func (o *Overlay) GetLeafData(ctx context.Context, id nodestore.NodeID) (io.ReadSeekCloser, error) {
	for i := len(o.Trees) - 1; i >= 0; i-- {
		node, err := o.Trees[i].Get(ctx, id)
		if err != nil {
			if errors.Is(err, nodestore.NodeNotFoundError{}) {
				continue
			}
			return nil, fmt.Errorf("failed to get node: %w", err)
		}
		leaf, ok := node.(*nodestore.LeafNode)
		if !ok {
			return nil, errors.New("node is not a leaf")
		}
		return util.NewReadSeekNopCloser(leaf.Data()), nil
	}
	return nil, nodestore.NodeNotFoundError{NodeID: id}
}

// NewOverlay creates a new TreeReader that overlays one TreeReader on top of another.
func NewOverlay(trees []TreeReader) TreeReader {
	return &Overlay{trees}
}
