package tree

import (
	"errors"
	"fmt"
	"log"
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

// Tree is a versioned, time-partitioned, COW Tree.
type Tree struct {
	depth   int
	bfactor int
	version uint64

	ns *nodestore.Nodestore

	rootmap map[uint64]nodestore.NodeID // todo: extract; version -> node id
}

type Stats struct {
	Depth           int
	BranchingFactor int
	LeafWidth       time.Duration
	Start           time.Time
	End             time.Time
}

func (t *Tree) Stats() (Stats, error) {
	root, err := t.root()
	if err != nil {
		return Stats{}, err
	}
	coverage := root.End - root.Start
	for i := 0; i < t.depth; i++ {
		coverage /= uint64(t.bfactor)
	}
	return Stats{
		Depth:           t.depth,
		BranchingFactor: t.bfactor,
		LeafWidth:       time.Duration(coverage * 1e9),
		Start:           time.Unix(int64(root.Start), 0),
		End:             time.Unix(int64(root.End), 0),
	}, nil
}

// NewTree constructs a new tree for the given start and end. The depth of the
// tree is computed such that the time span covered by each leaf node is at most
// leafWidthNanos.
func NewTree(
	start uint64,
	end uint64,
	leafWidthSecs uint64,
	branchingFactor int,
	nodeStore *nodestore.Nodestore,
) (*Tree, error) {
	minspan := end - start
	depth := 0
	coverage := leafWidthSecs
	for coverage < minspan {
		coverage *= uint64(branchingFactor)
		depth++
	}
	root := nodestore.NewInnerNode(start, start+coverage, branchingFactor)
	t := &Tree{
		depth:   depth,
		bfactor: branchingFactor,
		ns:      nodeStore,
		version: 0,
		rootmap: make(map[uint64]nodestore.NodeID),
	}
	rootID, err := t.stageNode(root)
	if err != nil {
		return nil, fmt.Errorf("failed to cache node: %w", err)
	}
	nodeIDs, err := t.ns.Flush(rootID)
	if err != nil {
		return nil, fmt.Errorf("failed to flush node: %w", err)
	}
	t.addRoot(0, nodeIDs[0]) // tree now functional for inserts
	return t, nil
}

// String returns a string representation of the tree.
func (t *Tree) String() string {
	root, err := t.root()
	if err != nil {
		log.Println("failed to get root:", err)
		return ""
	}
	s, err := t.printTree(root, t.version)
	if err != nil {
		log.Println("failed to print tree:", err)
		return ""
	}
	return s
}

// Insert the slice of data into the node corresponding to the given start time.
// The blob of data should be a well-formed MCAP file, and it must fit into
// exactly one leaf node. The caller is responsible for sectioning off MCAP
// files based on the configuration of the tree.
func (t *Tree) Insert(start uint64, data []byte) (nodeID nodestore.NodeID, err error) {
	currentRootID := t.rootmap[t.version]
	root, err := t.cloneInnerNode(currentRootID)
	if err != nil {
		return nodeID, err
	}
	version := t.version
	version++
	rootID, err := t.stageNode(root)
	if err != nil {
		return nodeID, err
	}
	nodes := make([]nodestore.NodeID, 0, t.depth)
	nodes = append(nodes, rootID)
	current := root
	for i := 0; i < t.depth-1; i++ {
		if current, err = t.descend(&nodes, current, start, version); err != nil {
			return nodeID, err
		}
	}
	// current is now the final parent
	bucket := t.bucket(start, current)
	var node *nodestore.LeafNode
	if existing := current.Children[bucket]; existing != nil {
		if node, err = t.cloneLeafNode(existing.ID, data); err != nil {
			return nodeID, err
		}
	} else {
		node = nodestore.NewLeafNode(data)
	}
	stagedID, err := t.stageNode(node)
	if err != nil {
		return nodeID, err
	}
	nodes = append(nodes, stagedID)
	current.PlaceChild(bucket, stagedID, version)
	nodeIDs, err := t.ns.Flush(nodes...)
	if err != nil {
		return nodeID, fmt.Errorf("failed to flush nodes: %w", err)
	}
	nodeID = nodeIDs[0]
	t.rootmap[version] = nodeID
	t.version = version
	return nodeID, nil
}

// addRoot adds a root node to the tree for the given version.
func (t *Tree) addRoot(version uint64, rootID nodestore.NodeID) {
	t.rootmap[version] = rootID
}

// bwidth returns the width of each bucket in seconds.
func (t *Tree) bwidth(n *nodestore.InnerNode) uint64 {
	return (n.End - n.Start) / uint64(t.bfactor)
}

// bucket returns the index of the child slot that the given time falls into on
// the given node.
func (t *Tree) bucket(nanos uint64, n *nodestore.InnerNode) uint64 {
	return (nanos/1e9 - n.Start) / t.bwidth(n)
}

// cloneInnerNode returns a new inner node with the same contents as the node
// with the given id, but with the given version.
func (t *Tree) cloneInnerNode(id nodestore.NodeID) (*nodestore.InnerNode, error) {
	node, err := t.ns.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to clone inner node %d: %w", id, err)
	}
	newNode := nodestore.NewInnerNode(0, 0, 0)
	oldNode, ok := node.(*nodestore.InnerNode)
	if !ok {
		return nil, errors.New("expected inner node - database is corrupt")
	}
	*newNode = *oldNode
	return newNode, nil
}

// cloneLeafNode returns a new leaf node with contents equal to the existing
// leaf node at the provide address, merged with the provided data.
func (t *Tree) cloneLeafNode(id nodestore.NodeID, data []byte) (*nodestore.LeafNode, error) {
	node, err := t.ns.Get(id)
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
func (t *Tree) root() (*nodestore.InnerNode, error) {
	root, err := t.ns.Get(t.rootmap[t.version])
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
func (t *Tree) stageNode(node nodestore.Node) (nodestore.NodeID, error) {
	id, err := t.ns.Stage(node)
	if err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to stage node %d: %w", id, err)
	}
	return id, nil
}

// descend descends the tree to the node that should contain the given timestamp.
func (t *Tree) descend(
	nodeIDs *[]nodestore.NodeID,
	current *nodestore.InnerNode,
	timestamp uint64,
	version uint64,
) (node *nodestore.InnerNode, err error) {
	bucket := t.bucket(timestamp, current)
	if existing := current.Children[bucket]; existing != nil {
		node, err = t.cloneInnerNode(existing.ID)
		if err != nil {
			return nil, err
		}
	} else {
		bwidth := t.bwidth(current)
		node = nodestore.NewInnerNode(
			current.Start+bucket*bwidth,
			current.Start+(bucket+1)*bwidth,
			t.bfactor,
		)
	}
	nodeID, err := t.stageNode(node)
	if err != nil {
		return nil, err
	}
	current.PlaceChild(bucket, nodeID, version)
	*nodeIDs = append(*nodeIDs, nodeID)
	return node, nil
}

// Bounds returns the time bounds of the leaf node that contains the given
// nanosecond timestamp. The units of the return value are seconds.
func (t *Tree) Bounds(ts uint64) ([]uint64, error) {
	root, err := t.root()
	if err != nil {
		return nil, err
	}
	width := root.End - root.Start
	for i := 0; i < t.depth; i++ {
		width /= uint64(t.bfactor)
	}
	inset := ts/1e9 - root.Start
	bucket := inset / width
	return []uint64{root.Start + width*bucket, root.Start + width*(bucket+1)}, nil
}

// printTree prints the tree rooted at the given node for test comparison.
func (t *Tree) printTree(node *nodestore.InnerNode, version uint64) (string, error) {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d:%d", node.Start, node.End, version))
	for i, slot := range node.Children {
		if slot == nil {
			continue
		}
		child, err := t.ns.Get(slot.ID)
		if err != nil {
			return "", fmt.Errorf("failed to get node %d: %w", slot.ID, err)
		}
		if child, ok := child.(*nodestore.LeafNode); ok {
			bwidth := t.bwidth(node)
			childStart := node.Start + uint64(i)*bwidth
			childEnd := node.Start + uint64(i+1)*bwidth
			sb.WriteString(" " + printLeaf(child, slot.Version, childStart, childEnd))
			continue
		}
		ichild, ok := child.(*nodestore.InnerNode)
		if !ok {
			return "", errors.New("expected inner node - database is corrupt")
		}
		childstr, err := t.printTree(ichild, slot.Version)
		if err != nil {
			return "", err
		}
		sb.WriteString(" " + childstr)
	}
	sb.WriteString("]")
	return sb.String(), nil
}

// printLeaf prints the given leaf node for test comparison.
func printLeaf(n *nodestore.LeafNode, version, start, end uint64) string {
	return fmt.Sprintf("[%d-%d:%d %s]", start, end, version, n)
}
