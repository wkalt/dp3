package tree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
)

var errElideNode = errors.New("elide node")

func Merge(
	ctx context.Context,
	w io.Writer,
	version uint64,
	dest TreeReader,
	inputs ...TreeReader,
) (nodestore.NodeID, error) {
	var destPair *util.Pair[TreeReader, nodestore.NodeID]
	if dest != nil {
		destPair = util.Pointer(util.NewPair(dest, dest.Root()))
	}
	inputPairs := make([]util.Pair[TreeReader, nodestore.NodeID], 0, len(inputs))
	for _, input := range inputs {
		inputPairs = append(inputPairs, util.NewPair(input, input.Root()))
	}
	cw := util.NewCountingWriter(w)
	nodeID, err := merge(ctx, version, cw, nil, destPair, inputPairs)
	if err != nil {
		return nodeID, fmt.Errorf("failed to merge roots: %w", err)
	}
	if _, err = w.Write(nodeID[:]); err != nil {
		return nodeID, fmt.Errorf("failed to write root node ID to object end: %w", err)
	}
	return nodeID, nil
}

func oneLeafMerge(
	ctx context.Context,
	cw *util.CountingWriter,
	version uint64,
	ancestorNodeID *nodestore.NodeID,
	ancestorVersion *uint64,
	input util.Pair[TreeReader, nodestore.NodeID],
) (nodestore.NodeID, error) {
	node, rsc, err := input.First.GetLeafNode(ctx, input.Second)
	if err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to get leaf: %w", err)
	}
	defer rsc.Close()
	newLeaf := nodestore.NewLeafNode(nil, ancestorNodeID, ancestorVersion)
	if node.AncestorDeleted() {
		if ancestorNodeID == nil {
			return nodestore.NodeID{}, errElideNode
		}
		newLeaf.DeleteRange(node.AncestorDeleteStart(), node.AncestorDeleteEnd())
	}
	offset := uint64(cw.Count())
	var length uint64

	// fake leaf node to write the header
	n, err := cw.Write(newLeaf.ToBytes())
	length += uint64(n)
	if err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to write leaf header to object: %w", err)
	}

	// copy the actual data
	m, err := io.Copy(cw, rsc)
	length += uint64(m)
	if err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to copy leaf data to object: %w", err)
	}
	nodeID := nodestore.NewNodeID(version, offset, length)
	return nodeID, nil
}

func getIterator(
	ctx context.Context,
	input util.Pair[TreeReader, nodestore.NodeID],
) (mcap.MessageIterator, func(), error) {
	tr := input.First
	leafID := input.Second
	node, err := tr.Get(ctx, leafID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get leaf iterator: %w", err)
	}
	leaf, ok := node.(*nodestore.LeafNode)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected node type: %T", node)
	}
	reader, err := mcap.NewReader(leaf.Data())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build leaf reader: %w", err)
	}
	iterator, err := reader.Messages()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create message iterator: %w", err)
	}
	return iterator, reader.Close, nil
}

func leafMerge(
	ctx context.Context,
	cw *util.CountingWriter,
	version uint64,
	destVersion *uint64,
	dest *util.Pair[TreeReader, nodestore.NodeID],
	inputs []util.Pair[TreeReader, nodestore.NodeID],
) (nodeID nodestore.NodeID, err error) {
	var ancestorNodeID *nodestore.NodeID
	var ancestorVersion *uint64
	if dest != nil {
		ancestorNodeID = &dest.Second
		ancestorVersion = destVersion
	}

	if len(inputs) == 0 {
		return nodeID, errors.New("no leaves to merge")
	}
	if len(inputs) == 1 {
		return oneLeafMerge(ctx, cw, version, ancestorNodeID, ancestorVersion, inputs[0])
	}

	// offset of the node start
	offset := uint64(cw.Count())

	// use a fake leaf node to write the header
	header := nodestore.NewLeafNode([]byte{}, ancestorNodeID, ancestorVersion)
	if err := header.Write(cw); err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to write leaf header: %w", err)
	}

	// build a list of message iterators
	iterators := make([]mcap.MessageIterator, len(inputs))
	for i, input := range inputs {
		iterator, finish, err := getIterator(ctx, input)
		if err != nil {
			return nodestore.NodeID{}, err
		}
		defer finish()
		iterators[i] = iterator
	}

	// merge iterators into output body
	if err := mcap.Nmerge(cw, iterators...); err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to merge leaf iterators: %w", err)
	}

	// compute the new node ID
	length := uint64(cw.Count()) - offset
	nodeID = nodestore.NewNodeID(version, offset, length)
	return nodeID, nil
}

func toNode[T *nodestore.InnerNode | *nodestore.LeafNode](
	ctx context.Context,
	pair util.Pair[TreeReader, nodestore.NodeID],
) (T, error) {
	tr := pair.First
	nodeID := pair.Second
	node, err := tr.Get(ctx, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s: %w", nodeID, err)
	}
	value, ok := node.(T)
	if !ok {
		return nil, fmt.Errorf("unexpected node type: %T", node)
	}
	return value, nil
}

// merge inner nodes constructs a new inner node with "merges" in the location
// of any "conflicts" among the children of the inputs.
func innerMerge( // nolint: funlen
	ctx context.Context,
	cw *util.CountingWriter,
	version uint64,
	dest *util.Pair[TreeReader, nodestore.NodeID],
	inputs []util.Pair[TreeReader, nodestore.NodeID],
) (nodeID nodestore.NodeID, err error) {
	if len(inputs) == 0 {
		return nodeID, errors.New("no children to merge")
	}
	nodes := make([]*nodestore.InnerNode, 0, len(inputs))
	for _, input := range inputs {
		node, err := toNode[*nodestore.InnerNode](ctx, input)
		if err != nil {
			return nodestore.NodeID{}, err
		}
		nodes = append(nodes, node)
	}

	// reference node to set dimensions of the output
	node := nodes[0]

	singleton := len(inputs) == 1

	// gather child indexes where conflicts occur
	conflicts := []int{}
	for i, child := range node.Children {
		// For a singleton node, all children must be treated as conflicts.
		if child != nil && singleton {
			conflicts = append(conflicts, i)
			continue
		}

		for _, sibling := range nodes[1:] {
			cousin := sibling.Children[i]
			if child == nil && cousin != nil ||
				child != nil && cousin == nil ||
				child != nil && cousin != nil {
				conflicts = append(conflicts, i)
				break // one sibling is enough to make a conflict
			}
		}
	}

	// Create a new inner node with same dimensions as the reference. In the
	// location of each conflict, compute a node ID via a recursive merge and
	// form a child structure from the aggregated statistics of the conflicted
	// children.
	newNode := nodestore.NewInnerNode(node.Height, node.Start, node.End, len(node.Children))

	// default the children of the new node, to those of the destination node,
	// if one exists.
	if dest != nil {
		destNode, err := toNode[*nodestore.InnerNode](ctx, *dest)
		if err != nil {
			return nodestore.NodeID{}, err
		}
		newNode.Children = destNode.Children
	}

	// build a merged child in the location of each conflict.
	for _, conflict := range conflicts {
		// sort the nodes by the version at the conflict location. This is used
		// for the tombstone handling below, to blank statistics/history when a
		// tombstone is encountered.
		sort.Slice(inputs, func(i, j int) bool {
			if nodes[i].Children[conflict] == nil || nodes[j].Children[conflict] == nil {
				return false
			}
			return nodes[i].Children[conflict].Version < nodes[j].Children[conflict].Version
		})
		sort.Slice(nodes, func(i, j int) bool {
			if nodes[i].Children[conflict] == nil || nodes[j].Children[conflict] == nil {
				return false
			}
			return nodes[i].Children[conflict].Version < nodes[j].Children[conflict].Version
		})

		var destChild *util.Pair[TreeReader, nodestore.NodeID]
		var destVersion *uint64
		statistics := map[string]*nodestore.Statistics{}
		if existing := newNode.Children[conflict]; existing != nil {
			destChild = util.Pointer(util.NewPair(dest.First, existing.ID))
			destVersion = &existing.Version
			for schemaHash, stats := range existing.Statistics {
				statistics[schemaHash] = stats.Clone()
			}
		}
		conflictedPairs := []util.Pair[TreeReader, nodestore.NodeID]{}
		maxVersion := uint64(0)

		for i, pair := range inputs {
			node := nodes[i]
			child := node.Children[conflict]
			if child == nil {
				continue
			}
			if child.Version > maxVersion {
				maxVersion = child.Version
			}

			// If this child is a tombstone, clear the statistics and history
			// and keep going forward. This could represent a delete followed by
			// inserts.
			if child.IsTombstone() {
				conflictedPairs = conflictedPairs[:0]
				clear(statistics)
				continue
			}
			conflictedPairs = append(
				conflictedPairs,
				util.NewPair(pair.First, child.ID),
			)
			for schemaHash, stats := range child.Statistics {
				if existing, ok := statistics[schemaHash]; ok {
					if err := existing.Add(stats); err != nil {
						return nodestore.NodeID{}, fmt.Errorf("failed to update statistics: %w", err)
					}
				} else {
					statistics[schemaHash] = stats.Clone()
				}
			}
		}

		if len(conflictedPairs) > 0 {
			mergedID, err := merge(ctx, version, cw, destVersion, destChild, conflictedPairs)
			if err != nil {
				if errors.Is(err, errElideNode) {
					continue
				}
				return nodestore.NodeID{}, fmt.Errorf("failed to merge nodes: %w", err)
			}
			newNode.Children[conflict] = &nodestore.Child{
				ID:         mergedID,
				Version:    version,
				Statistics: statistics,
			}
		} else {
			// If the final input was a tombstone, we need to blank any inherited from the destination
			newNode.Children[conflict] = nil
		}
	}

	// Now serialize the new inner node.
	data := newNode.ToBytes()
	offset := uint64(cw.Count())
	_, err = cw.Write(data)
	if err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to serialize inner node: %w", err)
	}
	length := uint64(cw.Count()) - offset
	return nodestore.NewNodeID(version, offset, length), nil
}

func merge(
	ctx context.Context,
	version uint64,
	w *util.CountingWriter,
	destVersion *uint64,
	dest *util.Pair[TreeReader, nodestore.NodeID],
	inputs []util.Pair[TreeReader, nodestore.NodeID],
) (nodestore.NodeID, error) {
	if len(inputs) == 0 {
		return nodestore.NodeID{}, errors.New("no nodes to merge")
	}
	pair := inputs[0]
	node, err := pair.First.Get(ctx, pair.Second)
	if err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to get node at %s: %w", pair.Second, err)
	}
	switch node := node.(type) {
	case *nodestore.LeafNode:
		return leafMerge(ctx, w, version, destVersion, dest, inputs)
	case *nodestore.InnerNode:
		return innerMerge(ctx, w, version, dest, inputs)
	default:
		return nodestore.NodeID{}, fmt.Errorf("unexpected node type: %T", node)
	}
}
