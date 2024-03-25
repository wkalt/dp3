package tree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
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

func getNode(
	ctx context.Context,
	id nodestore.NodeID,
	readers ...TreeReader,
) (remote bool, node nodestore.Node, err error) {
	for i, reader := range readers {
		node, err = reader.Get(ctx, id)
		if err != nil {
			if errors.Is(err, nodestore.NodeNotFoundError{}) {
				continue
			}
			return false, nil, fmt.Errorf("failed to get node: %w", err)
		}
		return i != 0, node, nil
	}
	return false, nil, nodestore.NodeNotFoundError{NodeID: id}
}

// Print a tree reader in human-readable format.
func Print(ctx context.Context, readers ...TreeReader) (string, error) {
	overlay := readers[0]
	node, err := overlay.Get(ctx, overlay.Root())
	if err != nil {
		return "", fmt.Errorf("failed to get root node: %w", err)
	}
	return printInnerNode(ctx, false, readers, node.(*nodestore.InnerNode), 0, nil)
}

func printInnerNode(
	ctx context.Context,
	remote bool,
	readers []TreeReader,
	node *nodestore.InnerNode,
	version uint64,
	stats *nodestore.Statistics,
) (string, error) {
	sb := &strings.Builder{}
	remotestr := util.When(remote, "<link> ", "")
	sb.WriteString(fmt.Sprintf("[%s%d-%d", remotestr, node.Start, node.End))
	if version > 0 {
		sb.WriteString(fmt.Sprintf(":%d %s", version, stats))
	}
	for i, child := range node.Children {
		if child == nil {
			continue
		}
		remote, childNode, err := getNode(ctx, child.ID, readers...)
		if err != nil && !errors.Is(err, nodestore.NodeNotFoundError{}) {
			return "", fmt.Errorf("failed to get node %d: %w", child.ID, err)
		}
		// the base reader was not included, so just print something useful
		if childNode == nil {
			width := (node.End - node.Start) / uint64(len(node.Children))
			sb.WriteString(fmt.Sprintf(" [<link> %d-%d:%d]",
				node.Start+uint64(i)*width,
				node.Start+uint64(i+1)*width,
				child.Version,
			))
			continue
		}
		var body string
		switch cnode := childNode.(type) {
		case *nodestore.InnerNode:
			sb.WriteString(" ")
			body, err = printInnerNode(ctx, remote, readers, cnode, child.Version, child.Statistics)
			if err != nil {
				return "", err
			}
			sb.WriteString(body)
		case *nodestore.LeafNode:
			width := (node.End - node.Start) / uint64(len(node.Children))
			body = cnode.String()
			remotestr := util.When(remote, "<link> ", "")
			sb.WriteString(fmt.Sprintf(" [%s%d-%d:%d %s %s]",
				remotestr,
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
