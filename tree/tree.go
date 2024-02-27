package tree

import (
	"fmt"
	"sort"
	"strings"

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
	root            *nodestore.InnerNode
	depth           int
	branchingFactor int

	ns *nodestore.Nodestore
}

func (t *Tree) bucketTime(time uint64) uint64 {
	bucketWidth := (t.root.End - t.root.Start) / pow(uint64(t.branchingFactor), t.depth)
	bucketIndex := (time - t.root.Start) / bucketWidth
	return bucketIndex * bucketWidth
}

func (t *Tree) insert(records []nodestore.Record) (err error) {
	var root = &nodestore.InnerNode{}
	*root = *t.root
	root.Version++
	txversion := root.Version
	groups := groupBy(records, func(r nodestore.Record) uint64 {
		return t.bucketTime(r.Time)
	})

	var newNodes []uint64
	for bucketTime, records := range groups {
		currentNode := root
		for i := 0; i < t.depth-1; i++ {
			bucket := t.getBucket(bucketTime, currentNode)
			var newChild *nodestore.InnerNode
			if childID := currentNode.Children[bucket]; childID > 0 {
				childNode, err := t.ns.Get(childID)
				if err != nil {
					return err
				}
				newChild = &nodestore.InnerNode{}
				*newChild = *childNode.(*nodestore.InnerNode)
				newChild.Version = txversion
			} else {
				newChild = nodestore.NewInnerNode(
					currentNode.Start+uint64(bucket)*t.bucketWidthNanos(currentNode),
					currentNode.Start+uint64(bucket+1)*t.bucketWidthNanos(currentNode),
					txversion,
					t.branchingFactor,
				)
			}
			newChildID, err := t.ns.Put(newChild)
			if err != nil {
				return err
			}
			currentNode.Children[bucket] = newChildID
			currentNode = newChild
			newNodes = append(newNodes, newChildID)
		}

		// currentNode is now the final parent
		newRecords := records
		bucket := t.getBucket(bucketTime, currentNode)
		if childID := currentNode.Children[bucket]; childID > 0 {
			leaf, err := t.ns.Get(childID)
			if err != nil {
				return err
			}
			old := leaf.(*nodestore.LeafNode)
			newRecords = append(old.Records, records...)
		}
		sort.Slice(newRecords, func(i, j int) bool {
			return newRecords[i].Time < newRecords[j].Time
		})
		new := nodestore.NewLeafNode(txversion, newRecords)
		newLeafID, err := t.ns.Put(new)
		if err != nil {
			return err
		}
		newNodes = append(newNodes, newLeafID)
		currentNode.Children[bucket] = newLeafID
	}
	for _, id := range newNodes {
		err := t.ns.Flush(id)
		if err != nil {
			return err
		}
	}
	rootID, err := t.ns.Put(root)
	if err != nil {
		return err
	}
	t.ns.Flush(rootID)
	t.root = root
	return nil
}

// NewTree constructs a new tree for the given start and end. The depth of the
// tree is computed such that the time span covered by each leaf node is at most
// leafWidthNanos.
func NewTree(
	start uint64,
	end uint64,
	leafWidthNanos uint64,
	branchingFactor int,
	ns *nodestore.Nodestore,
) *Tree {
	depth := 0
	for span := end - start; span > leafWidthNanos; depth++ {
		span = span / uint64(branchingFactor)
	}
	return &Tree{
		root:            nodestore.NewInnerNode(start, end, 0, branchingFactor),
		depth:           depth,
		branchingFactor: branchingFactor,
		ns:              ns,
	}
}

// bucketWidthNanos returns the width of each bucket in nanoseconds.
func (t *Tree) bucketWidthNanos(n *nodestore.InnerNode) uint64 {
	return (n.End - n.Start) / uint64(t.branchingFactor)
}

// getBucket returns the index of the bucket that the given time falls into.
func (t *Tree) getBucket(ts uint64, n *nodestore.InnerNode) int {
	return int((ts - n.Start) / t.bucketWidthNanos(n))
}

func printLeaf(n *nodestore.LeafNode, start, end uint64) string {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d:%d [leaf ", start, end, n.Version))
	for i, record := range n.Records {
		sb.WriteString(fmt.Sprintf("%d", record.Time))
		if i < len(n.Records)-1 {
			sb.WriteString(" ")
		}
	}
	sb.WriteString("]]")
	return sb.String()
}

func (t *Tree) printTree(n *nodestore.InnerNode) string {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d:%d", n.Start, n.End, n.Version))
	for i, childID := range n.Children {
		if childID == 0 {
			continue
		}
		child, err := t.ns.Get(childID)
		if err != nil {
			panic(err)
		}
		if child, ok := child.(*nodestore.LeafNode); ok {
			childStart := n.Start + uint64(i)*t.bucketWidthNanos(n)
			childEnd := n.Start + uint64(i+1)*t.bucketWidthNanos(n)
			sb.WriteString(" " + printLeaf(child, childStart, childEnd))
			continue
		}
		sb.WriteString(" " + t.printTree(child.(*nodestore.InnerNode)))
	}
	sb.WriteString("]")
	return sb.String()
}

func (t *Tree) String() string {
	return t.printTree(t.root)
}
