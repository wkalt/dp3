package tree

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/wkalt/dp3/nodestore"
)

/*
The tree reader interface is used to traverse trees stored in various ways -
either from memory, from WAL, or from the nodestore.
*/

////////////////////////////////////////////////////////////////////////////////

type TreeReader interface {
	Root() nodestore.NodeID
	Get(ctx context.Context, id nodestore.NodeID) (nodestore.Node, error)
	// todo: remove this in favor of a closure on leaf nodes.
	GetLeafData(ctx context.Context, id nodestore.NodeID) (io.ReadSeekCloser, error)
}

// Print a tree reader in human-readable format.
func Print(ctx context.Context, reader TreeReader) (string, error) {
	node, err := reader.Get(ctx, reader.Root())
	if err != nil {
		return "", fmt.Errorf("failed to get root node: %w", err)
	}
	return printInnerNode(ctx, reader, node.(*nodestore.InnerNode), 0, nil)
}

func printInnerNode(
	ctx context.Context,
	reader TreeReader,
	node *nodestore.InnerNode,
	version uint64,
	stats *nodestore.Statistics,
) (string, error) {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d", node.Start, node.End))
	if version > 0 {
		sb.WriteString(fmt.Sprintf(":%d %s", version, stats))
	}
	for i, child := range node.Children {
		if child == nil {
			continue
		}
		sb.WriteString(" ")
		childNode, err := reader.Get(ctx, child.ID)
		if err != nil {
			return "", fmt.Errorf("failed to get node %d: %w", child.ID, err)
		}
		var body string
		switch cnode := childNode.(type) {
		case *nodestore.InnerNode:
			body, err = printInnerNode(ctx, reader, cnode, child.Version, child.Statistics)
			if err != nil {
				return "", err
			}
			sb.WriteString(body)
		case *nodestore.LeafNode:
			width := (node.End - node.Start) / uint64(len(node.Children))
			body = cnode.String()
			sb.WriteString(fmt.Sprintf("[%d-%d:%d %s %s]",
				node.Start+uint64(i)*width,
				node.Start+uint64(i+1)*width,
				child.Version,
				child.Statistics,
				body,
			))
		}
	}
	sb.WriteString("]")
	return sb.String(), nil
}
