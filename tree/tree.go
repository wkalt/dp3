package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
)

/*
The tree package concerns maintenance of individual copy-on-write trees. Trees
are identified with a node ID and have no additional state. Operations on trees
return new root IDs.

The storage of dp3 consists of many trees, which are coordinated by the treemgr
module.

Note on integer overflows:
It is not difficult to construct a tree that spans into the future range of time
where uint64 nanoseconds will overflow, in 2554. While the inner nodes of that
tree, which are denominated in seconds, are fine, any math operations on those
boundaries or the computed boundaries of their children that involve multiplying
by 1e9 will cause a problem.

The guards against overflow are not as strong as they could be, but the way
things are currently written is that in the cases where we compute child
boundaries using nanosecond conversion, we always check first if there is a
child in the corresponding slot to begin, and skip over the child if so. By that
we are assured that any data that would cause an overflow was already written
into the tree somehow. Since the tree input is itself denominated in nanoseconds
and then cast down to seconds, there is no way to "write an overflowed timestamp
to the tree" -- whatever overflowed uint64 we get is just a uint64 from our
perspective.

I believe this all means that as long as you avoid computing start/end bounds of
children that do not exist, your code is safe from overflows.
*/

// //////////////////////////////////////////////////////////////////////////////

// NewInsertBranch writes the provided slice of data into a new (empty) tree writer, into
// the leaf of the tree that spans the requested timestamp. Assuming no error,
// after insert has returned, the tree writer will reflect a partial tree from
// root to leaf. The partial trees that result from insert are serialized to the
// WAL, and later merged into the main tree in batches by the tree manager.
//
// Note that the root is merely used as a template for determining the structure
// of the partial tree.
func NewInsertBranch(
	ctx context.Context,
	root *nodestore.InnerNode,
	version uint64,
	timestamp uint64,
	data []byte,
	statistics map[string]*nodestore.Statistics,
) (*MemTree, error) {
	if root == nil {
		return nil, errors.New("root is nil")
	}
	tw := NewMemTree(nodestore.RandomNodeID(), nil)
	if timestamp < root.Start*1e9 || timestamp >= root.End*1e9 {
		return nil, OutOfBoundsError{timestamp, root.Start, root.End}
	}
	clonedRoot := nodestore.NewInnerNode(
		root.Height,
		root.Start,
		root.End,
		len(root.Children),
	)
	ids := make([]nodestore.NodeID, 0, root.Height+1)
	for range root.Height + 1 {
		id := nodestore.RandomNodeID()
		ids = append(ids, id)
	}
	rootID := ids[0]
	if err := tw.Put(ctx, rootID, clonedRoot); err != nil {
		return nil, fmt.Errorf("failed to store new root: %w", err)
	}
	current := clonedRoot
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
			return nil, fmt.Errorf("failed to store inner node: %w", err)
		}
		current.PlaceChild(bucket, ids[i], version, statistics)
		current = node
	}
	// now at the parent of the leaf
	nodeID := ids[len(ids)-1]
	bucket := bucket(timestamp, current)
	node := nodestore.NewLeafNode(data, nil, nil)
	if err := tw.Put(ctx, nodeID, node); err != nil {
		return nil, fmt.Errorf("failed to store leaf node: %w", err)
	}
	current.PlaceChild(bucket, nodeID, version, statistics)
	tw.SetRoot(rootID)
	return tw, nil
}

// NewDeleteBranch constructs a partial tree that represents the deletion
// of messages in the given range. Start (inclusive) and end (exclusive) times
// are expected in nanoseconds. The tree writer returned by this function is
// expected to be serialized to the WAL and later merged into the main tree by
// the tree manager.
//
// Tombstones are placed in the tree at the location of children that are fully
// contained within the range. The partial tree, consisting of all nodes on the
// path to deletion (excluding the deleted nodes themselves, which have been
// tombstoned in their parents' children arrays), is returned.
//
// When a deletion range intersects a leaf node but does not fully cover it, the
// node is partially deleted. Since the deletion method does no IO with the
// existing tree, it has no way of knowing in this case whether there is data
// already existing in that location. We record an empty leaf node with a
// "delete range" set to the part of the node that should be deleted. During the
// merge step, there are then two cases to consider: either there was previously
// some data in that location, or there was not.
//
// If the location previously had leaf data, we add to the new leaf's Ancestor
// field the node ID of the previous leaf in the same location. On read, the
// tree iterator is responsible for navigating these linked lists and
// merging/skipping data as appropriate.
//
// If the location previously had no leaf data, the overlay is dropped in the
// merge step resulting in no new leaves to the tree.
//
// The root argument is merely used as a template for determining the structure
// of the partial tree -- no storage IO is performed in this function.
func NewDeleteBranch(
	ctx context.Context, oldroot *nodestore.InnerNode, version uint64, start uint64, end uint64,
) (*MemTree, error) {
	if start >= end {
		return nil, fmt.Errorf("invalid range: start %d cannot be >= end %d", start, end)
	}
	if start >= oldroot.End*1e9 || end < oldroot.Start*1e9 {
		return nil, fmt.Errorf("range [%d, %d) out of bounds [%d, %d)", start, end, oldroot.Start*1e9, oldroot.End*1e9)
	}
	root := nodestore.NewInnerNode(oldroot.Height, oldroot.Start, oldroot.End, len(oldroot.Children))
	tw := NewMemTree(nodestore.RandomNodeID(), root)
	stack := []*nodestore.InnerNode{root}
	leafData := &bytes.Buffer{}
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for i := range node.Children {
			childStart := node.Start + bwidth(node)*uint64(i)
			childEnd := node.Start + bwidth(node)*uint64(i+1)
			// The child is entirely outside the range.
			if start >= childEnd*1e9 || end <= childStart*1e9 {
				continue
			}
			// The entire child is within the range.
			if start <= childStart*1e9 && end >= childEnd*1e9 {
				node.PlaceTombstoneChild(uint64(i), version)
				continue
			}
			// Start, end, or both intersect the child. Treat both same as start.
			if node.Height > 1 {
				newChild := nodestore.NewInnerNode(node.Height-1, childStart, childEnd, len(node.Children))
				newChildID := nodestore.RandomNodeID()
				if err := tw.Put(ctx, newChildID, newChild); err != nil {
					return nil, fmt.Errorf("failed to store new inner node: %w", err)
				}
				if childStart*1e9 <= start && start < childEnd*1e9 {
					node.PlaceChild(bucket(start, node), newChildID, version, nil)
				} else {
					node.PlaceChild(bucket(end, node), newChildID, version, nil)
				}
				stack = append(stack, newChild)
				continue
			}
			// One level above the leaf node, deleting partial leaf.
			deleteRangeStart := max(start, childStart*1e9)
			deleteRangeEnd := min(end, childEnd*1e9)
			if err := writeEmptyMCAP(leafData); err != nil {
				return nil, fmt.Errorf("failed to write empty MCAP: %w", err)
			}
			leafID := nodestore.RandomNodeID()
			leaf := nodestore.NewLeafNode(leafData.Bytes(), nil, nil)
			leaf.DeleteRange(deleteRangeStart, deleteRangeEnd)
			if err := tw.Put(ctx, leafID, leaf); err != nil {
				return nil, fmt.Errorf("failed to store new leaf node: %w", err)
			}
			node.PlaceChild(bucket(deleteRangeStart, node), leafID, version, nil)
			leafData.Reset()
		}
	}
	return tw, nil
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
) ([]nodestore.StatRange, error) {
	ranges := []nodestore.StatRange{}
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
			width := bwidth(node)
			granularEnough := 1e9*width <= granularity
			for i, child := range node.Children {
				childStart := 1e9 * (node.Start + width*uint64(i))
				childEnd := 1e9 * (node.Start + width*uint64(i+1))
				inRange := child != nil && start <= childEnd && end > childStart
				if inRange && granularEnough {
					for schemaHash, statistics := range child.Statistics {
						ranges = append(ranges, statistics.Ranges(childStart, childEnd, schemaHash)...)
					}
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

// mergeInnerNodes merges a set of inner nodes into a single inner node. The
// nodes must all be of the same height. The resulting node is written to the
// provided tree writer. The resulting node is returned.
func mergeInnerNodes( // nolint: funlen // needs refactor
	ctx context.Context,
	pt TreeWriter,
	dest TreeReader,
	destID *nodestore.NodeID,
	pairs []util.Pair[*nodestore.InnerNode, *MemTree],
) (nodestore.Node, error) {
	conflicts := []int{}
	node := pairs[0].First
	singleton := len(pairs) == 1
	for i, child := range node.Children {
		if child != nil && singleton {
			conflicts = append(conflicts, i)
			continue
		}
		for _, sibling := range pairs[1:] {
			cousin := sibling.First.Children[i]
			if child == nil && cousin != nil ||
				child != nil && cousin == nil ||
				child != nil && cousin != nil {
				conflicts = append(conflicts, i)
				break
			}
		}
	}
	// Create a new merged child in the location of each conflict
	var destInnerNode *nodestore.InnerNode
	newInner := nodestore.NewInnerNode(node.Height, node.Start, node.End, len(node.Children))
	if destID != nil {
		destNode, err := dest.Get(ctx, *destID)
		if err != nil {
			return nil, fmt.Errorf("failed to get dest node: %w", err)
		}
		inner, ok := destNode.(*nodestore.InnerNode)
		if !ok {
			return nil, NewUnexpectedNodeError(nodestore.Inner, destNode)
		}
		destInnerNode = inner.Clone()
		newInner.Children = destInnerNode.Children
	}
	for _, conflict := range conflicts {
		statistics := map[string]*nodestore.Statistics{}
		var destChild *nodestore.NodeID
		var destChildVersion *uint64
		if destInnerNode != nil && destInnerNode.Children[conflict] != nil {
			child := destInnerNode.Children[conflict]
			for schemaHash, stats := range child.Statistics {
				statistics[schemaHash] = stats.Clone()
			}
			destChild = &child.ID
			destChildVersion = &child.Version
		}
		// Sort nodes by version of the conflicted child, if it exists.
		// This is necessary to ensure that the highest version is used.
		sort.Slice(pairs, func(i, j int) bool {
			left := pairs[i].First
			right := pairs[j].First
			if left.Children[conflict] == nil || right.Children[conflict] == nil {
				return false
			}
			return left.Children[conflict].Version < right.Children[conflict].Version
		})
		conflictedNodes := make([]util.Pair[nodestore.NodeID, *MemTree], 0, len(pairs))
		maxVersion := uint64(0)
		for _, pair := range pairs {
			child := pair.First.Children[conflict]
			if child == nil {
				continue
			}
			if child.Version > maxVersion {
				maxVersion = child.Version
			}
			// If the child is marked as deleted, ignore all versions up to, and including that, child.
			// TODO: reconcile statistics for tombstoned children.
			if child.IsTombstone() {
				conflictedNodes = conflictedNodes[:0]
				clear(statistics)
				continue
			}
			conflictedNodes = append(conflictedNodes, util.NewPair(child.ID, pair.Second))
			for schemaHash, stats := range child.Statistics {
				if existing, ok := statistics[schemaHash]; ok {
					if err := existing.Add(stats); err != nil {
						return nil, fmt.Errorf("failed to add statistics: %w", err)
					}
				} else {
					statistics[schemaHash] = stats.Clone()
				}
			}
		}
		if len(conflictedNodes) > 0 {
			merged, err := mergeLevel(ctx, pt, dest, destChild, destChildVersion, conflictedNodes)
			if err != nil {
				if errors.Is(err, errElideNode) {
					continue
				}
				return nil, err
			}
			newInner.Children[conflict] = &nodestore.Child{
				ID:         merged,
				Version:    maxVersion,
				Statistics: statistics,
			}
		} else {
			newInner.Children[conflict] = nil
		}
	}
	return newInner, nil
}

// mergeLevel merges the nodes provided into a single node of the same type. The
// nodes must all be of the same type. The ids argument must be in the same
// order as trees and in 1:1 correspondence. The resulting node is written to
// the provided tree writer. The resulting node is returned.
func mergeLevel(
	ctx context.Context,
	mt TreeWriter,
	dest TreeReader,
	destID *nodestore.NodeID,
	destVersion *uint64,
	pairs []util.Pair[nodestore.NodeID, *MemTree],
) (nodeID nodestore.NodeID, err error) {
	if len(pairs) == 0 {
		return nodeID, errors.New("no nodes to merge")
	}
	nodes := make([]nodestore.Node, 0, len(pairs))
	for _, pair := range pairs {
		node, err := pair.Second.Get(ctx, pair.First)
		if err != nil {
			return nodeID, fmt.Errorf("failed to get node: %w", err)
		}
		nodes = append(nodes, node)
	}
	for _, node := range nodes {
		if node.Type() != nodes[0].Type() {
			return nodeID, errors.New("mismatched node types")
		}
	}
	var node nodestore.Node
	switch nodes[0].Type() {
	case nodestore.Leaf:
		node, err = mergeLeaves(destID, destVersion, nodes)
		if err != nil {
			return nodeID, fmt.Errorf("failed to merge leaves: %w", err)
		}
	case nodestore.Inner:
		innerPairs := make([]util.Pair[*nodestore.InnerNode, *MemTree], len(pairs))
		for i, node := range nodes {
			innerPairs[i] = util.NewPair(node.(*nodestore.InnerNode), pairs[i].Second)
		}
		node, err = mergeInnerNodes(ctx, mt, dest, destID, innerPairs)
		if err != nil {
			return nodeID, fmt.Errorf("failed to merge inner nodes: %w", err)
		}
	}

	id := nodestore.RandomNodeID()
	if err := mt.Put(ctx, id, node); err != nil {
		return nodeID, fmt.Errorf("failed to insert node: %w", err)
	}

	return id, nil
}

// MergeBranchesInto merges the trees provided into the output tree. The output tree is
// expected to be empty. The trees are merged in the order they are provided.
// The trees must all have the same height. The output tree will have the same
// height as the input trees.
func MergeBranchesInto(
	ctx context.Context,
	dest TreeReader,
	trees ...*MemTree,
) (*MemTree, error) {
	output := NewMemTree(nodestore.RandomNodeID(), nil)
	if len(trees) == 0 {
		return nil, errors.New("no trees to merge")
	}
	destRoot := dest.Root()
	pairs := make([]util.Pair[nodestore.NodeID, *MemTree], 0, len(trees))
	roots := make([]*nodestore.InnerNode, 0, len(trees))
	for _, tree := range trees {
		id := tree.Root()
		root, err := tree.Get(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("failed to get root: %w", err)
		}
		innerNode, ok := root.(*nodestore.InnerNode)
		if !ok {
			return nil, NewUnexpectedNodeError(nodestore.Inner, root)
		}
		roots = append(roots, innerNode)
		pairs = append(pairs, util.NewPair(id, tree))
	}
	for _, root := range roots {
		if root.Height != roots[0].Height {
			return nil, MismatchedHeightsError{root.Height, roots[0].Height}
		}
	}
	mergedRoot, err := mergeLevel(ctx, output, dest, &destRoot, nil, pairs)
	if err != nil {
		return nil, fmt.Errorf("failed to merge: %w", err)
	}
	output.SetRoot(mergedRoot)
	return output, nil
}

var errElideNode = errors.New("elide node")

func mergeOneLeaf(
	ancestorNodeID *nodestore.NodeID,
	ancestorVersion *uint64,
	leaf *nodestore.LeafNode,
) (nodestore.Node, error) {
	data, err := io.ReadAll(leaf.Data())
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}
	newLeaf := nodestore.NewLeafNode(data, ancestorNodeID, ancestorVersion)
	if leaf.AncestorDeleted() {
		// if we have an ancestor deleted here, but we have no ancestor node ID,
		// then we are dealing with a deletion that spans unpopulated data. In
		// this scenario we don't want to create a leaf node in the merged
		// output.
		if ancestorNodeID == nil {
			return nil, errElideNode
		}
		newLeaf.DeleteRange(leaf.AncestorDeleteStart(), leaf.AncestorDeleteEnd())
	}
	return newLeaf, nil
}

// mergeLeaves merges a set of leaf nodes into a single leaf node. Data is
// merged in timestamp order.
func mergeLeaves(
	ancestorNodeID *nodestore.NodeID,
	ancestorVersion *uint64,
	leaves []nodestore.Node,
) (nodestore.Node, error) {
	if len(leaves) == 0 {
		return nil, errors.New("no leaves to merge")
	}
	if len(leaves) == 1 {
		leaf, ok := leaves[0].(*nodestore.LeafNode)
		if !ok {
			return nil, NewUnexpectedNodeError(nodestore.Leaf, leaf)
		}
		return mergeOneLeaf(ancestorNodeID, ancestorVersion, leaf)
	}

	iterators := make([]mcap.MessageIterator, len(leaves))
	for i, leaf := range leaves {
		leaf, ok := leaf.(*nodestore.LeafNode)
		if !ok {
			return nil, NewUnexpectedNodeError(nodestore.Leaf, leaf)
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
	return nodestore.NewLeafNode(buf.Bytes(), ancestorNodeID, ancestorVersion), nil
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

func writeEmptyMCAP(buf *bytes.Buffer) error {
	writer, err := mcap.NewWriter(buf)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	if err := writer.WriteHeader(&fmcap.Header{}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}
