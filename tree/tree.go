package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
)

// todo
// * tree dimensions (branching factor, depth) must be chosen at runtime based
//    on some initial samples of the data and a target leaf node size.
// * tree currently assumes no nodes it is working with get evicted from cache
// over course of an insert. not safe. separate cache for working nodes may be
// needed.

type TreeStats struct {
	Depth           uint8
	BranchingFactor int
	LeafWidth       time.Duration
	Start           time.Time
	End             time.Time
}

func Stats(ctx context.Context, ns *nodestore.Nodestore, rootID nodestore.NodeID) (TreeStats, error) {
	root, err := ns.Get(ctx, rootID)
	if err != nil {
		return TreeStats{}, err
	}
	node := root.(*nodestore.InnerNode)
	coverage := node.End - node.Start
	for i := uint8(0); i < node.Depth; i++ {
		coverage /= uint64(len(node.Children))
	}
	return TreeStats{
		Depth:           node.Depth,
		BranchingFactor: len(node.Children),
		LeafWidth:       time.Duration(coverage * 1e9),
		Start:           time.Unix(int64(node.Start), 0),
		End:             time.Unix(int64(node.End), 0),
	}, nil
}

// Insert the slice of data into the node corresponding to the given start time.
// The blob of data should be a well-formed MCAP file, and it must fit into
// exactly one leaf node. The caller is responsible for sectioning off MCAP
// files based on the configuration of the tree.
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
	rootID, err = stageNode(ns, root)
	if err != nil {
		return rootID, nil, err
	}
	nodes := make([]nodestore.NodeID, 0)
	nodes = append(nodes, rootID)
	current := root
	depth := root.Depth
	for current.Depth > 1 {
		if current, err = descend(ctx, ns, &nodes, current, start, version); err != nil {
			return rootID, nil, err
		}
		depth--
	}
	// current is now the final parent
	bucket := bucket(start, current)
	if bucket > uint64(len(current.Children)-1) {
		return rootID, nil, fmt.Errorf("bucket %d is out of range", bucket)
	}
	var node *nodestore.LeafNode
	if existing := current.Children[bucket]; existing != nil {
		if node, err = cloneLeafNode(ctx, ns, existing.ID, data); err != nil {
			return rootID, nil, err
		}
	} else {
		node = nodestore.NewLeafNode(data)
	}
	stagedID, err := stageNode(ns, node)
	if err != nil {
		return rootID, nil, err
	}
	nodes = append(nodes, stagedID)
	current.PlaceChild(bucket, stagedID, version)
	//nodeIDs, err := ns.Flush(ctx, nodes...)
	//if err != nil {
	//	return rootID, nil, fmt.Errorf("failed to flush nodes: %w", err)
	//}
	return nodes[0], nodes, nil
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
	if bucket > uint64(len(n.Children)-1) {
		fmt.Println("wud")
	}
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
func cloneLeafNode(ctx context.Context, ns *nodestore.Nodestore, id nodestore.NodeID, data []byte) (*nodestore.LeafNode, error) {
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

// root returns the root node of the tree.
func root(ctx context.Context, ns *nodestore.Nodestore, rootID nodestore.NodeID) (*nodestore.InnerNode, error) {
	root, err := ns.Get(ctx, rootID)
	if err != nil {
		return nil, fmt.Errorf("failed to get root: %w", err)
	}
	rootnode, ok := root.(*nodestore.InnerNode)
	if !ok {
		return nil, errors.New("expected inner node - database is corrupt")
	}
	return rootnode, nil
}

// stageNode stages the given node in the nodestore and returns its ID.
func stageNode(ns *nodestore.Nodestore, node nodestore.Node) (nodestore.NodeID, error) {
	id, err := ns.Stage(node)
	if err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to stage node %d: %w", id, err)
	}
	return id, nil
}

// descend descends the tree to the node that should contain the given timestamp.
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
	nodeID, err := stageNode(ns, node)
	if err != nil {
		return nil, err
	}
	current.PlaceChild(bucket, nodeID, version)
	*nodeIDs = append(*nodeIDs, nodeID)
	return node, nil
}

// Bounds returns the time bounds of the leaf node that contains the given
// nanosecond timestamp. The units of the return value are seconds.
func Bounds(ctx context.Context, ns *nodestore.Nodestore, rootID nodestore.NodeID, ts uint64) ([]uint64, error) {
	root, err := root(ctx, ns, rootID)
	if err != nil {
		return nil, err
	}
	width := root.End - root.Start
	for i := 0; i < int(root.Depth); i++ {
		width /= uint64(len(root.Children))
	}
	inset := ts/1e9 - root.Start
	bucket := inset / width
	return []uint64{root.Start + width*bucket, root.Start + width*(bucket+1)}, nil
}

func mergeLeaves(ctx context.Context, ns *nodestore.Nodestore, nodeIDs []nodestore.NodeID) (nodestore.NodeID, error) {
	if len(nodeIDs) == 1 {
		return nodeIDs[0], nil
	}
	iterators := make([]fmcap.MessageIterator, len(nodeIDs))
	var ok bool
	var err error
	var leaf nodestore.Node
	for i, id := range nodeIDs {
		if leaf, ok = ns.GetStagedNode(id); !ok {
			if leaf, err = ns.Get(ctx, id); err != nil {
				return nodestore.NodeID{}, fmt.Errorf("failed to get leaf %d", id)
			}
		}
		reader, err := fmcap.NewReader(leaf.(*nodestore.LeafNode).Data())
		if err != nil {
			return nodestore.NodeID{}, fmt.Errorf("failed to create reader: %w", err)
		}
		defer reader.Close()
		iterators[i], err = reader.Messages()
		if err != nil {
			return nodestore.NodeID{}, fmt.Errorf("failed to create iterator: %w", err)
		}
	}
	buf := &bytes.Buffer{}
	if err := mcap.Nmerge(buf, iterators...); err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to merge leaves: %w", err)
	}
	newLeaf := nodestore.NewLeafNode(buf.Bytes())
	return stageNode(ns, newLeaf)
}

// NodeMerge does an N-way tree merge, returning a "path" from the root to leaf
// of the new tree. All nodes are from the same level of the tree, and can thus
// be assumed to have the same type and same number of children.
func NodeMerge(ctx context.Context, ns *nodestore.Nodestore, version uint64, nodeIDs []nodestore.NodeID) ([]nodestore.NodeID, error) {
	if len(nodeIDs) == 0 {
		return nil, errors.New("no nodes to merge")
	}
	var err error
	var ok bool
	nodes := make([]nodestore.Node, len(nodeIDs))
	for i, id := range nodeIDs {
		if nodes[i], ok = ns.GetStagedNode(id); !ok {
			if nodes[i], err = ns.Get(ctx, id); err != nil {
				return nil, fmt.Errorf("failed to get node %d", id)
			}
		}
	}
	switch node := (nodes[0]).(type) {
	case *nodestore.InnerNode:
		conflicts := []int{}
		for i, outer := range node.Children {
			var conflicted bool
			for _, node := range nodes {
				inner := (node).(*nodestore.InnerNode).Children[i]
				if outer == nil && inner != nil ||
					outer != nil && inner == nil ||
					outer != nil && inner != nil && outer.ID != inner.ID {
					conflicted = true
				}
				if conflicted {
					conflicts = append(conflicts, i)
				}
			}
		}
		newInner := nodestore.NewInnerNode(node.Depth, node.Start, node.End, len(node.Children))
		newID, err := ns.Stage(newInner)
		if err != nil {
			return nil, err
		}
		result := []nodestore.NodeID{newID}
		for _, conflict := range conflicts {
			children := []nodestore.NodeID{} // set of not-null children mapping to conflicts
			for _, node := range nodes {
				if inner := (node).(*nodestore.InnerNode).Children[conflict]; inner != nil {
					children = append(children, inner.ID)
				}
			}
			merged, err := NodeMerge(ctx, ns, version, children) // merged child for this conflict
			if err != nil {
				return nil, err
			}
			newInner.Children[conflict] = &nodestore.Child{ID: merged[0], Version: version}
			result = append(result, merged...)
		}

		for i := range node.Children {
			if !slices.Contains(conflicts, i) {
				newInner.Children[i] = node.Children[i]
			}
		}
		return result, nil
	case *nodestore.LeafNode:
		merged, err := mergeLeaves(ctx, ns, nodeIDs)
		if err != nil {
			return nil, err
		}
		return []nodestore.NodeID{merged}, nil
	default:
		return nil, fmt.Errorf("unrecognized node type: %T", node)
	}
}

func PrintTree(ctx context.Context, ns *nodestore.Nodestore, nodeID nodestore.NodeID, version uint64) (string, error) {
	sb := &strings.Builder{}
	var node nodestore.Node
	var ok bool
	var err error
	if node, ok = ns.GetStagedNode(nodeID); !ok {
		node, err = ns.Get(ctx, nodeID)
		if err != nil {
			return "", fmt.Errorf("failed to get node %d: %w", nodeID, err)
		}
	}
	switch node := node.(type) {
	case *nodestore.InnerNode:
		sb.WriteString(fmt.Sprintf("[%d-%d:%d", node.Start, node.End, version))
		span := node.End - node.Start
		for i, child := range node.Children {
			if child == nil {
				continue
			}
			childstr, err := PrintTree(ctx, ns, child.ID, child.Version)
			if err != nil {
				return "", err
			}
			var childNode nodestore.Node
			var ok bool
			if childNode, ok = ns.GetStagedNode(child.ID); !ok {
				childNode, err = ns.Get(ctx, child.ID)
				if err != nil {
					return "", fmt.Errorf("failed to get node %d: %w", child.ID, err)
				}
			}
			if cnode, ok := childNode.(*nodestore.LeafNode); ok {
				start := node.Start + uint64(i)*span/uint64(len(node.Children))
				end := node.Start + uint64(i+1)*span/uint64(len(node.Children))
				sb.WriteString(fmt.Sprintf(" [%d-%d:%d %s]", start, end, child.Version, cnode))
			} else {
				sb.WriteString(" " + childstr)
			}
		}
		sb.WriteString("]")
	case *nodestore.LeafNode:
		sb.WriteString(node.String())
	}
	return sb.String(), nil
}
