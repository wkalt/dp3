package tree

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
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
	bucketWidth := (t.root.End - t.root.Start) / util.Pow(uint64(t.branchingFactor), t.depth)
	bucketIndex := (time - t.root.Start) / bucketWidth
	return bucketIndex * bucketWidth
}

//nolint:funlen // need to refactor
func (t *Tree) Insert(records []nodestore.Record) (err error) {
	var root = nodestore.NewInnerNode(0, 0, 0, 0)
	*root = *t.root
	root.Version++
	txversion := root.Version
	groups := util.GroupBy(records, func(r nodestore.Record) uint64 {
		return t.bucketTime(r.Time)
	})
	newNodes := make([]uint64, 0, t.depth*len(groups))
	for bucketTime, records := range groups {
		currentNode := root
		for i := 0; i < t.depth-1; i++ {
			bucket := t.getBucket(bucketTime, currentNode)
			var newChild *nodestore.InnerNode
			if childID := currentNode.Children[bucket]; childID > 0 {
				childNode, err := t.ns.Get(childID)
				if err != nil {
					return fmt.Errorf("failed to get node %d: %w", childID, err)
				}
				newChild = nodestore.NewInnerNode(0, 0, 0, 0)
				childNodeInnerNode, ok := childNode.(*nodestore.InnerNode)
				if !ok {
					return errors.New("expected inner node - database is corrupt")
				}
				*newChild = *childNodeInnerNode
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
				return fmt.Errorf("failed to cache node %d: %w", newChildID, err)
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
				return fmt.Errorf("failed to get node %d: %w", childID, err)
			}
			old, ok := leaf.(*nodestore.LeafNode)
			if !ok {
				return errors.New("expected leaf node - database is corrupt")
			}
			newRecords = old.Records
			newRecords = append(newRecords, records...)
		}
		sort.Slice(newRecords, func(i, j int) bool {
			return newRecords[i].Time < newRecords[j].Time
		})
		newLeafNode := nodestore.NewLeafNode(txversion, newRecords)
		newLeafID, err := t.ns.Put(newLeafNode)
		if err != nil {
			return fmt.Errorf("failed to cache node %d: %w", newLeafID, err)
		}
		newNodes = append(newNodes, newLeafID)
		currentNode.Children[bucket] = newLeafID
	}
	for _, id := range newNodes {
		err := t.ns.Flush(id)
		if err != nil {
			return fmt.Errorf("failed to flush node %d: %w", id, err)
		}
	}
	rootID, err := t.ns.Put(root)
	if err != nil {
		return fmt.Errorf("failed to cache root node: %w", err)
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
	nodeStore *nodestore.Nodestore,
) *Tree {
	depth := 0
	for span := end - start; span > leafWidthNanos; depth++ {
		span /= uint64(branchingFactor)
	}
	return &Tree{
		root:            nodestore.NewInnerNode(start, end, 0, branchingFactor),
		depth:           depth,
		branchingFactor: branchingFactor,
		ns:              nodeStore,
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
		sb.WriteString(strconv.FormatUint(record.Time, 10))
		if i < len(n.Records)-1 {
			sb.WriteString(" ")
		}
	}
	sb.WriteString("]]")
	return sb.String()
}

func (t *Tree) printTree(node *nodestore.InnerNode) (string, error) {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d:%d", node.Start, node.End, node.Version))
	for i, childID := range node.Children {
		if childID == 0 {
			continue
		}
		child, err := t.ns.Get(childID)
		if err != nil {
			return "", fmt.Errorf("failed to get node %d: %w", childID, err)
		}
		if child, ok := child.(*nodestore.LeafNode); ok {
			childStart := node.Start + uint64(i)*t.bucketWidthNanos(node)
			childEnd := node.Start + uint64(i+1)*t.bucketWidthNanos(node)
			sb.WriteString(" " + printLeaf(child, childStart, childEnd))
			continue
		}
		ichild, ok := child.(*nodestore.InnerNode)
		if !ok {
			return "", fmt.Errorf("expected inner node - database is corrupt")
		}
		childstr, err := t.printTree(ichild)
		if err != nil {
			return "", err
		}
		sb.WriteString(" " + childstr)
	}
	sb.WriteString("]")
	return sb.String(), nil
}

func (t *Tree) String() string {
	s, err := t.printTree(t.root)
	if err != nil {
		log.Println("failed to print tree:", err)
	}
	return s
}
