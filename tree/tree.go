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
	root    *nodestore.InnerNode
	depth   int
	bfactor int
	version uint64

	ns *nodestore.Nodestore
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
		root:    nodestore.NewInnerNode(start, end, branchingFactor),
		depth:   depth,
		bfactor: branchingFactor,
		ns:      nodeStore,
		version: 0,
	}
}

//nolint:funlen // need to refactor
func (t *Tree) Insert(records []nodestore.Record) (err error) {
	var root = nodestore.NewInnerNode(0, 0, 0)
	*root = *t.root
	version := t.version
	version++
	groups := util.GroupBy(records, func(r nodestore.Record) uint64 {
		return t.bucketTime(r.Time)
	})
	nodes := make([]uint64, 0, t.depth*len(groups))
	for bucketTime, records := range groups {
		current := root
		for i := 0; i < t.depth-1; i++ {
			bucket := t.bucket(bucketTime, current)
			var node *nodestore.InnerNode
			if existing := current.Children[bucket]; existing.ID > 0 {
				node, err = t.cloneInnerNode(existing.ID)
				if err != nil {
					return err
				}
			} else {
				bwidth := t.bwidth(current)
				node = nodestore.NewInnerNode(
					current.Start+bucket*bwidth,
					current.Start+(bucket+1)*bwidth,
					t.bfactor,
				)
			}
			nodeID, err := t.cacheNode(node)
			if err != nil {
				return err
			}
			current.PlaceChild(bucket, nodeID, version)
			current = node
			nodes = append(nodes, nodeID)
		}

		// currentNode is now the final parent
		bucket := t.bucket(bucketTime, current)
		var node *nodestore.LeafNode
		if existing := current.Children[bucket]; existing.ID > 0 {
			node, err = t.cloneLeafNode(existing.ID)
			if err != nil {
				return err
			}
			node.Records = append(node.Records, records...)
		} else {
			node = nodestore.NewLeafNode(records)
		}
		sort.Slice(node.Records, func(i, j int) bool {
			return node.Records[i].Time < node.Records[j].Time
		})
		nodeID, err := t.cacheNode(node)
		if err != nil {
			return err
		}
		nodes = append(nodes, nodeID)
		current.PlaceChild(bucket, nodeID, version)
	}
	for _, id := range nodes {
		err := t.ns.Flush(id)
		if err != nil {
			return fmt.Errorf("failed to flush node %d: %w", id, err)
		}
	}
	rootID, err := t.cacheNode(root)
	if err != nil {
		return err
	}
	t.ns.Flush(rootID)
	t.root = root
	t.version = version
	return nil
}

// bwidth returns the width of each bucket in nanoseconds.
func (t *Tree) bwidth(n *nodestore.InnerNode) uint64 {
	return (n.End - n.Start) / uint64(t.bfactor)
}

// bucket returns the index of the child slot that the given time falls into on
// the given node.
func (t *Tree) bucket(ts uint64, n *nodestore.InnerNode) uint64 {
	return (ts - n.Start) / t.bwidth(n)
}

// printLeaf prints the given leaf node for test comparison.
func printLeaf(n *nodestore.LeafNode, version, start, end uint64) string {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d:%d [leaf ", start, end, version))
	for i, record := range n.Records {
		sb.WriteString(strconv.FormatUint(record.Time, 10))
		if i < len(n.Records)-1 {
			sb.WriteString(" ")
		}
	}
	sb.WriteString("]]")
	return sb.String()
}

// cloneInnerNode returns a new inner node with the same contents as the node
// with the given id, but with the given version.
func (t *Tree) cloneInnerNode(id uint64) (*nodestore.InnerNode, error) {
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

func (t *Tree) cloneLeafNode(id uint64) (*nodestore.LeafNode, error) {
	node, err := t.ns.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to clone leaf node %d: %w", id, err)
	}
	newNode := nodestore.NewLeafNode(nil)
	oldNode, ok := node.(*nodestore.LeafNode)
	if !ok {
		return nil, errors.New("expected leaf node - database is corrupt")
	}
	*newNode = *oldNode
	return newNode, nil
}

// printTree prints the tree rooted at the given node for test comparison.
func (t *Tree) printTree(node *nodestore.InnerNode, version uint64) (string, error) {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d:%d", node.Start, node.End, version))
	for i, slot := range node.Children {
		if slot.ID == 0 {
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

// String returns a string representation of the tree.
func (t *Tree) String() string {
	s, err := t.printTree(t.root, t.version)
	if err != nil {
		log.Println("failed to print tree:", err)
	}
	return s
}

func (t *Tree) bucketTime(time uint64) uint64 {
	bucketWidth := (t.root.End - t.root.Start) / util.Pow(uint64(t.bfactor), t.depth)
	bucketIndex := (time - t.root.Start) / bucketWidth
	return bucketIndex * bucketWidth
}

func (t *Tree) cacheNode(node nodestore.Node) (uint64, error) {
	id, err := t.ns.Put(node)
	if err != nil {
		return 0, fmt.Errorf("failed to cache node %d: %w", id, err)
	}
	return id, nil
}
