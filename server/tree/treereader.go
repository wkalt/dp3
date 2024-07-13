package tree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/nodestore"
	"github.com/wkalt/dp3/server/util"
)

/*
The tree reader interface is used to traverse trees stored in various ways -
either from memory, from WAL, or from the nodestore.
*/

////////////////////////////////////////////////////////////////////////////////

type Reader interface {
	Root() nodestore.NodeID
	Get(ctx context.Context, id nodestore.NodeID) (nodestore.Node, error)
	GetLeafNode(ctx context.Context, id nodestore.NodeID) (*nodestore.LeafNode, io.ReadSeekCloser, error)
}

func getNode(
	ctx context.Context,
	id nodestore.NodeID,
	readers ...Reader,
) (remote bool, node nodestore.Node, err error) {
	for i, reader := range readers {
		if reader == nil {
			continue
		}
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
func Print(ctx context.Context, readers ...Reader) (string, error) {
	overlay := readers[0]
	node, err := overlay.Get(ctx, overlay.Root())
	if err != nil {
		return "", fmt.Errorf("failed to get root node: %w", err)
	}
	inner, ok := node.(*nodestore.InnerNode)
	if !ok {
		return "", errors.New("root node is not an inner node")
	}
	return printInnerNode(ctx, false, readers, inner, 0, nil)
}

func printStats(stats map[string]*nodestore.Statistics) string {
	sb := &strings.Builder{}
	sb.WriteString("(")
	first := true
	for _, schemaHash := range util.Okeys(stats) {
		if !first {
			sb.WriteString(" ")
		}
		statistics := stats[schemaHash]
		prefix := schemaHash[:2]
		sb.WriteString(fmt.Sprintf("%s %s", prefix, statistics))
		first = false
	}
	sb.WriteString(")")
	return sb.String()
}

func printLeaf(
	ctx context.Context,
	readers []Reader,
	node *nodestore.LeafNode,
) (string, error) {
	sb := &strings.Builder{}
	reader, err := fmcap.NewReader(node.Data())
	if err != nil {
		return "", fmt.Errorf("failed to create reader: %w", err)
	}
	info, err := reader.Info()
	if err != nil {
		return fmt.Sprintf("[leaf <unknown %d bytes>]", node.Size()), nil // nolint:nilerr
	}
	if info.Statistics.MessageCount == 1 {
		sb.WriteString("[leaf 1 msg]")
	} else {
		sb.WriteString(fmt.Sprintf("[leaf %d msgs]", info.Statistics.MessageCount))
	}

	switch {
	case node.HasAncestor() && node.AncestorDeleted():
		sb.WriteString(fmt.Sprintf("-<del %d-%d>->", node.AncestorDeleteStart()/1e9, node.AncestorDeleteEnd()/1e9))
	case node.AncestorDeleted():
		// this is for when a node has deleted but not been merged with an existing tree - so the ancestor is unpopulated.
		sb.WriteString(fmt.Sprintf("-<del %d %d>-> ??", node.AncestorDeleteStart()/1e9, node.AncestorDeleteEnd()/1e9))
	case node.HasAncestor():
		sb.WriteString("->")
	}

	if node.HasAncestor() {
		_, ancestor, err := getNode(ctx, node.Ancestor(), readers...)
		if err != nil {
			return "", fmt.Errorf("failed to get ancestor: %w", err)
		}
		s, err := printLeaf(ctx, readers, ancestor.(*nodestore.LeafNode))
		if err != nil {
			return "", err
		}
		sb.WriteString(s)
	}
	return sb.String(), nil
}

func printInnerNode(
	ctx context.Context,
	remote bool,
	readers []Reader,
	node *nodestore.InnerNode,
	version uint64,
	stats map[string]*nodestore.Statistics,
) (string, error) {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%s%d-%d", util.When(remote, "<ref> ", ""), node.Start, node.End))
	if version > 0 {
		sb.WriteString(fmt.Sprintf(":%d %s", version, printStats(stats)))
	}
	for i, child := range node.Children {
		if child == nil {
			continue
		}
		relationship := util.When(child.IsTombstone(), "<del>", "<ref>")
		remote, childNode, err := getNode(ctx, child.ID, readers...)
		if err != nil && !errors.Is(err, nodestore.NodeNotFoundError{}) {
			return "", fmt.Errorf("failed to get node %d: %w", child.ID, err)
		}
		// the base reader was not included, so just print something useful
		if childNode == nil {
			width := (node.End - node.Start) / uint64(len(node.Children))
			sb.WriteString(fmt.Sprintf(" [%s %d-%d:%d]",
				relationship,
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
			body, err := printLeaf(ctx, readers, cnode)
			if err != nil {
				return "", err
			}
			sb.WriteString(fmt.Sprintf(" [%s%d-%d:%d %s %s]",
				util.When(remote, relationship+" ", ""),
				node.Start+uint64(i)*width,
				node.Start+uint64(i+1)*width,
				child.Version,
				printStats(child.Statistics),
				body,
			))
		}
	}
	sb.WriteString("]")
	return sb.String(), nil
}
