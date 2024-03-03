package tree

import (
	"errors"
	"fmt"
	"strings"
	"time"

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

func Stats(ns *nodestore.Nodestore, rootID nodestore.NodeID) (TreeStats, error) {
	root, err := ns.Get(rootID)
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

func flushOne(ns *nodestore.Nodestore, n nodestore.Node) (nodeID nodestore.NodeID, err error) {
	id, err := ns.Stage(n)
	if err != nil {
		return nodeID, fmt.Errorf("failed to stage node: %w", err)
	}
	nodeIDs, err := ns.Flush(id)
	if err != nil {
		return nodeID, fmt.Errorf("failed to flush node: %w", err)
	}
	nodeID = nodeIDs[0]
	return nodeID, nil
}

// Insert the slice of data into the node corresponding to the given start time.
// The blob of data should be a well-formed MCAP file, and it must fit into
// exactly one leaf node. The caller is responsible for sectioning off MCAP
// files based on the configuration of the tree.
func Insert(
	ns *nodestore.Nodestore,
	nodeID nodestore.NodeID,
	version uint64,
	start uint64,
	data []byte,
) (rootID nodestore.NodeID, path []nodestore.NodeID, err error) {
	root, err := cloneInnerNode(ns, nodeID)
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
		if current, err = descend(ns, &nodes, current, start, version); err != nil {
			return rootID, nil, err
		}
		depth--
	}
	// current is now the final parent
	bucket := bucket(start, current)
	var node *nodestore.LeafNode
	if existing := current.Children[bucket]; existing != nil {
		if node, err = cloneLeafNode(ns, existing.ID, data); err != nil {
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
	nodeIDs, err := ns.Flush(nodes...)
	if err != nil {
		return rootID, nil, fmt.Errorf("failed to flush nodes: %w", err)
	}
	return nodeIDs[0], nodeIDs, nil
}

// bwidth returns the width of each bucket in seconds.
func bwidth(n *nodestore.InnerNode) uint64 {
	return (n.End - n.Start) / uint64(len(n.Children))
}

// bucket returns the index of the child slot that the given time falls into on
// the given node.
func bucket(nanos uint64, n *nodestore.InnerNode) uint64 {
	return (nanos/1e9 - n.Start) / bwidth(n)
}

// cloneInnerNode returns a new inner node with the same contents as the node
// with the given id, but with the given version.
func cloneInnerNode(ns *nodestore.Nodestore, id nodestore.NodeID) (*nodestore.InnerNode, error) {
	node, err := ns.Get(id)
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
func cloneLeafNode(ns *nodestore.Nodestore, id nodestore.NodeID, data []byte) (*nodestore.LeafNode, error) {
	node, err := ns.Get(id)
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
func root(ns *nodestore.Nodestore, rootID nodestore.NodeID) (*nodestore.InnerNode, error) {
	root, err := ns.Get(rootID)
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
	ns *nodestore.Nodestore,
	nodeIDs *[]nodestore.NodeID,
	current *nodestore.InnerNode,
	timestamp uint64,
	version uint64,
) (node *nodestore.InnerNode, err error) {
	bucket := bucket(timestamp, current)
	if existing := current.Children[bucket]; existing != nil {
		node, err = cloneInnerNode(ns, existing.ID)
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
func Bounds(ns *nodestore.Nodestore, rootID nodestore.NodeID, ts uint64) ([]uint64, error) {
	root, err := root(ns, rootID)
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

func PrintTree(ns *nodestore.Nodestore, nodeID nodestore.NodeID, version uint64) (string, error) {
	sb := &strings.Builder{}
	node, err := ns.Get(nodeID)
	if err != nil {
		return "", fmt.Errorf("failed to get node %d: %w", nodeID, err)
	}
	switch node := node.(type) {
	case *nodestore.InnerNode:
		sb.WriteString(fmt.Sprintf("[%d-%d:%d", node.Start, node.End, version))
		span := node.End - node.Start
		for i, child := range node.Children {
			if child == nil {
				continue
			}
			childstr, err := PrintTree(ns, child.ID, child.Version)
			if err != nil {
				return "", err
			}
			childNode, err := ns.Get(child.ID)
			if err != nil {
				return "", fmt.Errorf("failed to get node %d: %w", child.ID, err)
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
		sb.WriteString(fmt.Sprintf("%s", node))
	}
	return sb.String(), nil
}
