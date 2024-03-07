package tree

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/wkalt/dp3/nodestore"
)

func Insert(
	ctx context.Context,
	ns *nodestore.Nodestore,
	nodeID nodestore.NodeID,
	version uint64,
	start uint64,
	data []byte,
) (rootID nodestore.NodeID, path []nodestore.NodeID, err error) {
	root, err := cloneInnerNode(ctx, ns, nodeID)
	if err != nil {
		return rootID, nil, err
	}
	rootID = ns.Stage(root)
	nodes := []nodestore.NodeID{rootID}
	current := root
	for current.Depth > 1 {
		current, err = descend(ctx, ns, &nodes, current, start, version)
		if err != nil {
			return rootID, nil, err
		}
	}
	bucket := bucket(start, current)
	if bucket > uint64(len(current.Children)-1) {
		return rootID, nil, fmt.Errorf("bucket %d is out of range", bucket)
	}
	var node *nodestore.LeafNode
	if existing := current.Children[bucket]; existing != nil {
		node, err = cloneLeafNode(ctx, ns, existing.ID, data)
		if err != nil {
			return rootID, nil, err
		}
	} else {
		node = nodestore.NewLeafNode(data)
	}
	stagedID := ns.Stage(node)
	nodes = append(nodes, stagedID)
	current.PlaceChild(bucket, stagedID, version)
	return nodes[0], nodes, nil
}

func Print(ctx context.Context, ns *nodestore.Nodestore, nodeID nodestore.NodeID, version uint64) (string, error) {
	node, err := ns.Get(ctx, nodeID)
	if err != nil {
		return "", fmt.Errorf("failed to get node %d: %w", nodeID, err)
	}
	switch node := node.(type) {
	case *nodestore.InnerNode:
		return printInnerNode(ctx, ns, node, version)
	case *nodestore.LeafNode:
		return node.String(), nil
	default:
		return "", errors.New("unexpected node type")
	}
}

func printInnerNode(
	ctx context.Context,
	ns *nodestore.Nodestore,
	node *nodestore.InnerNode,
	version uint64,
) (string, error) {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d:%d", node.Start, node.End, version))
	span := node.End - node.Start
	for i, child := range node.Children {
		if child == nil {
			continue
		}
		childStr, err := Print(ctx, ns, child.ID, child.Version)
		if err != nil {
			return "", err
		}
		childNode, err := ns.Get(ctx, child.ID)
		if err != nil {
			return "", fmt.Errorf("failed to get node %d: %w", child.ID, err)
		}
		if cnode, ok := childNode.(*nodestore.LeafNode); ok {
			start := node.Start + uint64(i)*span/uint64(len(node.Children))
			end := node.Start + uint64(i+1)*span/uint64(len(node.Children))
			sb.WriteString(fmt.Sprintf(" [%d-%d:%d %s]", start, end, child.Version, cnode))
		} else {
			sb.WriteString(" " + childStr)
		}
	}
	sb.WriteString("]")
	return sb.String(), nil
}

// bwidth returns the width of each bucket in seconds.
func bwidth(n *nodestore.InnerNode) uint64 {
	bwidth := (n.End - n.Start) / uint64(len(n.Children))
	return bwidth
}

// bucket returns the index of the child slot that the given time falls into on
// the given node.
func bucket(nanos uint64, n *nodestore.InnerNode) uint64 {
	bwidth := bwidth(n)
	bucket := (nanos - n.Start*1e9) / (1e9 * bwidth)
	return bucket
}

// cloneInnerNode returns a new inner node with the same contents as the node
// with the given id, but with the given version.
func cloneInnerNode(ctx context.Context, ns *nodestore.Nodestore, id nodestore.NodeID) (*nodestore.InnerNode, error) {
	node, err := ns.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to clone inner node %d: %w", id, err)
	}
	newNode := nodestore.NewInnerNode(0, 0, 0, 0)
	oldNode, ok := node.(*nodestore.InnerNode)
	if !ok {
		return nil, errors.New("expected inner node - database is corrupt")
	}
	*newNode = *oldNode
	return newNode, nil
}

// cloneLeafNode returns a new leaf node with contents equal to the existing
// leaf node at the provide address, merged with the provided data.
func cloneLeafNode(
	ctx context.Context,
	ns *nodestore.Nodestore,
	id nodestore.NodeID,
	data []byte,
) (*nodestore.LeafNode, error) {
	node, err := ns.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to clone leaf node %d: %w", id, err)
	}
	oldNode, ok := node.(*nodestore.LeafNode)
	if !ok {
		return nil, errors.New("expected data node - database is corrupt")
	}
	merged, err := oldNode.Merge(data)
	if err != nil {
		return nil, fmt.Errorf("failed to clone node: %w", err)
	}
	return merged, nil
}

// descend the tree to the node that contains the given timestamp, copying nodes
// at each step and recording the path taken.
func descend(
	ctx context.Context,
	ns *nodestore.Nodestore,
	nodeIDs *[]nodestore.NodeID,
	current *nodestore.InnerNode,
	timestamp uint64,
	version uint64,
) (node *nodestore.InnerNode, err error) {
	bucket := bucket(timestamp, current)
	if existing := current.Children[bucket]; existing != nil {
		node, err = cloneInnerNode(ctx, ns, existing.ID)
		if err != nil {
			return nil, err
		}
	} else {
		bwidth := bwidth(current)
		node = nodestore.NewInnerNode(
			current.Depth-1,
			current.Start+bucket*bwidth,
			current.Start+(bucket+1)*bwidth,
			len(current.Children),
		)
	}
	nodeID := ns.Stage(node)
	current.PlaceChild(bucket, nodeID, version)
	*nodeIDs = append(*nodeIDs, nodeID)
	return node, nil
}
