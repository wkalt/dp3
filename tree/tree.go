package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
)

/*
The tree package concerns maintenance of individual copy-on-write trees. Trees
are identified with a node ID and have no additional state. Operations on trees
return new root IDs.

The storage of dp3 consists of many trees, which are coordinated by the treemgr
module.
*/

// //////////////////////////////////////////////////////////////////////////////

// Insert writes the provided slice of data into the supplied tree writer, into
// the leaf of the tree that spans the requested timestamp. Assuming no error,
// after insert has returned the tree writer will reflect a partial tree from
// root to leaf. In practice the tree writer passed to insert is always a
// "memtree". The partial trees that result from insert are serialized to the
// WAL, and later merged into the main tree in batches by the tree manager.
//
// Accordingly and confusingly, "insert" is only ever called on empty trees, and
// the test helpers for insertion combine it with a merge operation to produce
// more interesting examples.
func Insert(
	ctx context.Context,
	tw TreeWriter,
	version uint64,
	timestamp uint64,
	data []byte,
	statistics *nodestore.Statistics,
) error {
	oldRootID := tw.Root()
	oldRootNode, err := tw.Get(ctx, oldRootID)
	if err != nil {
		return nodestore.NodeNotFoundError{NodeID: oldRootID}
	}
	oldroot, ok := oldRootNode.(*nodestore.InnerNode)
	if !ok || oldroot == nil {
		return newUnexpectedNodeError(nodestore.Inner, oldroot)
	}
	if timestamp < oldroot.Start*1e9 || timestamp >= oldroot.End*1e9 {
		return OutOfBoundsError{timestamp, oldroot.Start, oldroot.End}
	}
	root := nodestore.NewInnerNode(
		oldroot.Height,
		oldroot.Start,
		oldroot.End,
		len(oldroot.Children),
	)
	ids := make([]nodestore.NodeID, 0, oldroot.Height+1)
	for range oldroot.Height + 1 {
		id := nodestore.RandomNodeID()
		ids = append(ids, id)
	}
	rootID := ids[0]
	if err := tw.Put(ctx, rootID, root); err != nil {
		return fmt.Errorf("failed to store new root: %w", err)
	}
	current := root
	for i := 1; current.Height > 1; i++ {
		bucket := bucket(timestamp, current)
		bwidth := bwidth(current)
		node := nodestore.NewInnerNode(
			current.Height-1,
			current.Start+bucket*bwidth,
			current.Start+(bucket+1)*bwidth,
			len(current.Children),
		)
		if err := tw.Put(ctx, ids[i], node); err != nil {
			return fmt.Errorf("failed to store inner node: %w", err)
		}
		current.PlaceChild(bucket, ids[i], version, statistics)
		current = node
	}
	// now at the parent of the leaf
	nodeID := ids[len(ids)-1]
	bucket := bucket(timestamp, current)
	node := nodestore.NewLeafNode(data)
	if err := tw.Put(ctx, nodeID, node); err != nil {
		return fmt.Errorf("failed to store leaf node: %w", err)
	}
	current.PlaceChild(bucket, nodeID, version, statistics)
	tw.SetRoot(rootID)
	return nil
}

// StatRange is a range of statistics.
type StatRange struct {
	Start      uint64                `json:"start"`
	End        uint64                `json:"end"`
	Statistics *nodestore.Statistics `json:"statistics"`
}

// GetStatRange returns the statistics for the given range of time, for the tree
// rooted at rootID. The granularity parameter is interpreted as a "maximum
// granularity". The returned granularity is guaranteed to be at least as fine
// as the one requested, and in practice can be considerably finer. This can
// lead to confusing results so clients must be prepared to handle it.
func GetStatRange(
	ctx context.Context,
	tr TreeReader,
	start uint64,
	end uint64,
	granularity uint64,
) ([]StatRange, error) {
	ranges := []StatRange{}
	stack := []nodestore.NodeID{tr.Root()}
	for len(stack) > 0 {
		nodeID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node, err := tr.Get(ctx, nodeID)
		if err != nil {
			return nil, fmt.Errorf("failed to get node %s: %w", nodeID, err)
		}
		switch node := node.(type) {
		case *nodestore.InnerNode:
			granularEnough := 1e9*bwidth(node) <= granularity
			width := bwidth(node)
			for i, child := range node.Children {
				childStart := 1e9 * (node.Start + width*uint64(i))
				childEnd := 1e9 * (node.Start + width*uint64(i+1))
				inRange := child != nil && start <= childEnd && end > childStart
				if inRange && granularEnough {
					ranges = append(ranges, StatRange{
						Start:      childStart,
						End:        childEnd,
						Statistics: child.Statistics,
					})
					continue
				}
				if inRange {
					stack = append(stack, child.ID)
				}
			}
		case *nodestore.LeafNode:
			// todo: compute exact statistics from the messages
			return nil, errors.New("sorry, too granular")
		}
	}
	return ranges, nil
}

// Merge a list of TreeReaders into an empty TreeWriter. At each level of
// conflict, merged nodes are versioned with the highest version of the
// conflicted inputs.
func Merge(ctx context.Context, tw TreeWriter, trees ...TreeReader) error {
	if len(trees) == 0 {
		return nil
	}
	ids := make([]nodestore.NodeID, 0, len(trees))
	roots := make([]*nodestore.InnerNode, 0, len(trees))
	for _, tree := range trees {
		id := tree.Root()
		root, err := tree.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("failed to get root: %w", err)
		}
		innerNode, ok := root.(*nodestore.InnerNode)
		if !ok {
			return newUnexpectedNodeError(nodestore.Inner, root)
		}
		roots = append(roots, innerNode)
		ids = append(ids, id)
	}
	for _, root := range roots[1:] {
		if root.Height != roots[0].Height {
			return MismatchedHeightsError{root.Height, roots[0].Height}
		}
	}
	mergedRoot, err := mergeLevel(ctx, tw, ids, trees)
	if err != nil {
		return fmt.Errorf("failed to merge: %w", err)
	}
	tw.SetRoot(mergedRoot)
	return nil
}

// mergeInnerNodes merges a list of inner nodes into a single inner node in the
// new tree writer. It does this by identifying "conflicted" child indexes, and
// then calling mergeLevel on each of the conflicted sets of children. The
// mergeLevel function will either dispatch back to mergeInnerNodes, or to
// mergeLeaves depending on the height at which it is called.

// The passed nodes array must correspond in order and match in length with the
// supplied list of tree readers.
func mergeInnerNodes(
	ctx context.Context,
	pt TreeWriter,
	nodes []*nodestore.InnerNode,
	trees []TreeReader,
) (nodestore.Node, error) {
	conflicts := []int{}
	node := nodes[0]
	singleton := len(nodes) == 1
	for i, child := range node.Children {
		if child != nil && singleton {
			conflicts = append(conflicts, i)
			continue
		} else {
			var conflicted bool
			for _, sibling := range nodes[1:] {
				cousin := sibling.Children[i]
				if child == nil && cousin != nil ||
					child != nil && cousin == nil ||
					child != nil && cousin != nil {
					conflicted = true
					break
				}
			}
			if conflicted {
				conflicts = append(conflicts, i)
			}
		}
	}
	// Create a new merged child in the location of each conflict
	newInner := nodestore.NewInnerNode(node.Height, node.Start, node.End, len(node.Children))
	for _, conflict := range conflicts {
		conflictedNodes := make([]nodestore.NodeID, 0, len(trees))
		conflictedTrees := make([]TreeReader, 0, len(trees))
		stats := &nodestore.Statistics{}
		maxVersion := uint64(0)
		for i, node := range nodes {
			child := node.Children[conflict]
			if child == nil {
				continue
			}
			if child.Version > maxVersion {
				maxVersion = child.Version
			}
			conflictedNodes = append(conflictedNodes, child.ID)
			conflictedTrees = append(conflictedTrees, trees[i])
			if err := stats.Add(child.Statistics); err != nil {
				return nil, fmt.Errorf("failed to add statistics: %w", err)
			}
		}
		merged, err := mergeLevel(ctx, pt, conflictedNodes, conflictedTrees)
		if err != nil {
			return nil, err
		}
		newInner.Children[conflict] = &nodestore.Child{
			ID:         merged,
			Version:    maxVersion,
			Statistics: stats,
		}
	}
	return newInner, nil
}

// mergeLeaves merges a set of leaf nodes into a single leaf node. Data is
// merged in timestamp order.
func mergeLeaves(
	leaves []nodestore.Node,
) (nodestore.Node, error) {
	if len(leaves) == 0 {
		return nil, errors.New("no leaves to merge")
	}
	if len(leaves) == 1 {
		return leaves[0], nil
	}
	iterators := make([]fmcap.MessageIterator, len(leaves))
	for i, leaf := range leaves {
		leaf, ok := leaf.(*nodestore.LeafNode)
		if !ok {
			return nil, newUnexpectedNodeError(nodestore.Leaf, leaf)
		}
		reader, err := mcap.NewReader(leaf.Data())
		if err != nil {
			return nil, fmt.Errorf("failed to build mcap reader: %w", err)
		}
		defer reader.Close()
		iterators[i], err = reader.Messages()
		if err != nil {
			return nil, fmt.Errorf("failed to create iterator: %w", err)
		}
	}
	buf := &bytes.Buffer{}
	if err := mcap.Nmerge(buf, iterators...); err != nil {
		return nil, fmt.Errorf("failed to merge: %w", err)
	}
	return nodestore.NewLeafNode(buf.Bytes()), nil
}

// mergeLevel recursively merges a set of nodes at the same level in their
// respective input trees starting at the roots.  If all passed nodes are not of
// the same type and height (for inner nodes), an error will result. The final
// merged result is placed into the output tree writer with a random node ID.
func mergeLevel(
	ctx context.Context,
	pt TreeWriter,
	ids []nodestore.NodeID,
	trees []TreeReader,
) (nodeID nodestore.NodeID, err error) {
	if len(ids) == 0 {
		return nodeID, errors.New("no nodes to merge")
	}
	nodes := make([]nodestore.Node, len(ids))
	for i, id := range ids {
		node, err := trees[i].Get(ctx, id)
		if err != nil {
			return nodeID, fmt.Errorf("failed to get node: %w", err)
		}
		nodes[i] = node
	}
	for _, node := range nodes {
		if node.Type() != nodes[0].Type() {
			return nodeID, errors.New("mismatched node types")
		}
	}
	var node nodestore.Node
	switch nodes[0].Type() {
	case nodestore.Leaf:
		node, err = mergeLeaves(nodes)
		if err != nil {
			return nodeID, fmt.Errorf("failed to merge leaves: %w", err)
		}
	case nodestore.Inner:
		innerNodes := make([]*nodestore.InnerNode, len(nodes))
		for i, node := range nodes {
			innerNodes[i] = node.(*nodestore.InnerNode)
		}
		node, err = mergeInnerNodes(ctx, pt, innerNodes, trees)
		if err != nil {
			return nodeID, fmt.Errorf("failed to merge inner nodes: %w", err)
		}
	}
	id := nodestore.RandomNodeID()
	if err := pt.Put(ctx, id, node); err != nil {
		return nodeID, fmt.Errorf("failed to insert node: %w", err)
	}
	return id, nil
}

// // bwidth returns the width of each bucket in seconds.
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
