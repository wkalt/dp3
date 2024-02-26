package tree

import (
	"fmt"
	"sort"
	"strings"
)

// todo
// * tree dimensions (branching factor, depth) must be chosen at runtime based
//    on some initial samples of the data and a target leaf node size.

// Tree is a versioned, time-partitioned, COW Tree.
type Tree struct {
	root            *innerNode
	depth           int
	branchingFactor int
}

func (t *Tree) bucketTime(time uint64) uint64 {
	bucketWidth := (t.root.end - t.root.start) / pow(uint64(t.branchingFactor), t.depth)
	bucketIndex := (time - t.root.start) / bucketWidth
	return bucketIndex * bucketWidth
}

func (t *Tree) insert(records []record) {
	var root = &innerNode{}
	*root = *t.root
	root.version++
	txversion := root.version
	groups := groupBy(records, func(r record) uint64 {
		return t.bucketTime(r.time)
	})
	for bucketTime, records := range groups {
		currentNode := root
		for i := 0; i < t.depth-1; i++ {
			bucket := t.getBucket(bucketTime, currentNode)
			var child = &innerNode{}
			if currentNode.children[bucket] == nil {
				child = &innerNode{
					start:    currentNode.start + uint64(bucket)*t.bucketWidthNanos(currentNode),
					end:      currentNode.start + uint64(bucket+1)*t.bucketWidthNanos(currentNode),
					children: make([]node, t.branchingFactor),
				}
			} else {
				*child = *currentNode.children[bucket].(*innerNode)
			}
			child.setVersion(txversion)
			currentNode.children[bucket] = child
			currentNode = child
		}
		bucket := t.getBucket(bucketTime, currentNode)
		if currentNode.children[bucket] == nil {
			currentNode.children[bucket] = &leafNode{}
		}
		leaf := currentNode.children[bucket].(*leafNode)
		newRecords := append(leaf.records, records...)
		sort.Slice(newRecords, func(i, j int) bool {
			return newRecords[i].time < newRecords[j].time
		})
		leaf.records = newRecords
		leaf.setVersion(txversion)
	}
	t.root = root
}

// record represents a timestamped byte array
type record struct {
	time uint64
	data []byte
}

// NewTree constructs a new tree for the given start and end. The depth of the
// tree is computed such that the time span covered by each leaf node is at most
// leafWidthNanos.
func NewTree(start uint64, end uint64, leafWidthNanos uint64, branchingFactor int) *Tree {
	depth := 0
	for span := end - start; span > leafWidthNanos; depth++ {
		span = span / uint64(branchingFactor)
	}
	return &Tree{
		root: &innerNode{
			start:    start,
			end:      end,
			children: make([]node, branchingFactor),
		},
		depth:           depth,
		branchingFactor: branchingFactor,
	}
}

// node is an interface to which leaf and inner nodes adhere.
type node interface {
	// isLeaf indicates whether the node is a leaf node
	isLeaf() bool

	// setVersion sets the version of the node
	setVersion(version uint64)
}

// innerNode represents an interior node in the tree, with slots for 64
// children.
type innerNode struct {
	start    uint64
	end      uint64
	children []node
	version  uint64
}

// setVersion sets the version of the node.
func (n *innerNode) setVersion(version uint64) {
	n.version = version
}

// isLeaf indicates whether the node is a leaf node.
func (n *innerNode) isLeaf() bool {
	return false
}

// bucketWidthNanos returns the width of each bucket in nanoseconds.
func (t *Tree) bucketWidthNanos(n *innerNode) uint64 {
	return (n.end - n.start) / uint64(t.branchingFactor)
}

// getBucket returns the index of the bucket that the given time falls into.
func (t *Tree) getBucket(ts uint64, n *innerNode) int {
	return int((ts - n.start) / t.bucketWidthNanos(n))
}

// leafNode represents a leaf node in the tree, containing records.
type leafNode struct {
	version uint64
	records []record
}

// setVersion sets the version of the node.
func (n *leafNode) setVersion(version uint64) {
	n.version = version
}

// isLeaf indicates whether the node is a leaf node.
func (n *leafNode) isLeaf() bool {
	return true
}

func printLeaf(n *leafNode, start, end uint64) string {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d:%d [leaf ", start, end, n.version))
	for i, record := range n.records {
		sb.WriteString(fmt.Sprintf("%d", record.time))
		if i < len(n.records)-1 {
			sb.WriteString(" ")
		}
	}
	sb.WriteString("]]")
	return sb.String()
}

func (t *Tree) printTree(n *innerNode) string {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d:%d", n.start, n.end, n.version))
	for i, child := range n.children {
		if child == nil {
			continue
		}
		if child.isLeaf() {
			childStart := n.start + uint64(i)*t.bucketWidthNanos(n)
			childEnd := n.start + uint64(i+1)*t.bucketWidthNanos(n)
			sb.WriteString(" " + printLeaf(child.(*leafNode), childStart, childEnd))
			continue
		}
		sb.WriteString(" " + t.printTree(child.(*innerNode)))
	}
	sb.WriteString("]")
	return sb.String()
}

func (t *Tree) String() string {
	return t.printTree(t.root)
}
