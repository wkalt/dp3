package tree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strings"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/nodestore"
	"github.com/wkalt/dp3/server/util"
	"github.com/wkalt/dp3/server/util/ros1msg"
	"github.com/wkalt/dp3/server/util/schema"
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

	nodeID, _, err := mergeInnerNode(ctx, cw, version, destPair, inputPairs)
	if err != nil {
		return nodeID, fmt.Errorf("failed to partial trees roots: %w", err)
	}
	if _, err = w.Write(nodeID[:]); err != nil {
		return nodeID, fmt.Errorf("failed to write root node ID to object end: %w", err)
	}
	return nodeID, nil
}

func getHeader(
	ctx context.Context,
	input util.Pair[TreeReader, nodestore.NodeID],
) (*nodestore.LeafNode, error) {
	tr := input.First
	leafID := input.Second
	header, r, err := tr.GetLeafNode(ctx, leafID)
	if err != nil {
		return nil, fmt.Errorf("failed to get leaf iterator: %w", err)
	}
	if err := r.Close(); err != nil {
		return nil, fmt.Errorf("failed to close leaf reader: %w", err)
	}
	return header, nil
}

func getIterator(
	ctx context.Context,
	input util.Pair[TreeReader, nodestore.NodeID],
) (mcap.MessageIterator, func() error, error) {
	tr := input.First
	leafID := input.Second
	_, r, err := tr.GetLeafNode(ctx, leafID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get leaf iterator: %w", err)
	}
	reader, err := mcap.NewReader(r)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build leaf reader: %w", err)
	}
	iterator, err := reader.Messages()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create message iterator: %w", err)
	}
	closer := func() error {
		reader.Close()
		if err := r.Close(); err != nil {
			return fmt.Errorf("failed to close leaf reader: %w", err)
		}
		return nil
	}
	return iterator, closer, nil
}

func onMessageCallback() (
	map[string]*nodestore.Statistics,
	func(*fmcap.Schema, *fmcap.Channel, *fmcap.Message) error,
) {
	schemaStats := make(map[string]*nodestore.Statistics)
	parsers := make(map[uint16]*schema.Parser)
	schemaHashes := make(map[uint16]string)
	return schemaStats, func(s *fmcap.Schema, c *fmcap.Channel, m *fmcap.Message) error {
		parser, ok := parsers[s.ID]
		if !ok {
			parts := strings.SplitN(s.Name, "/", 2)
			var pkg, name string
			if len(parts) == 2 {
				pkg = parts[0]
				name = parts[1]
			}
			parsed, err := ros1msg.ParseROS1MessageDefinition(pkg, name, s.Data)
			if err != nil {
				return fmt.Errorf("failed to parse ROS1 message definition: %w", err)
			}
			fields := schema.AnalyzeSchema(*parsed)
			colnames := make([]string, len(fields))
			for i, field := range fields {
				colnames[i] = field.Name
			}
			parser, err = schema.NewParser(parsed, colnames, ros1msg.NewDecoder(nil))
			if err != nil {
				return fmt.Errorf("failed to create parser: %w", err)
			}
			parsers[s.ID] = parser
			schemaHashes[s.ID] = util.CryptographicHash(s.Data)
			schemaStats[schemaHashes[s.ID]] = nodestore.NewStatistics(fields)
		}
		stats := schemaStats[schemaHashes[s.ID]]
		_, values, err := parser.Parse(m.Data)
		if err != nil {
			return fmt.Errorf("failed to parse message: %w", err)
		}
		if err := stats.ObserveMessage(m, values); err != nil {
			return fmt.Errorf("failed to observe message: %w", err)
		}
		return nil
	}
}

func buildFilter(
	ctx context.Context,
	tr TreeReader,
	leafID nodestore.NodeID,
) (*nodestore.LeafNode, []nodestore.MessageKey, error) {
	nodes := []*nodestore.LeafNode{}
	leaf, rsc, err := tr.GetLeafNode(ctx, leafID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get leaf: %w", err)
	}
	if err := rsc.Close(); err != nil {
		return nil, nil, fmt.Errorf("failed to close leaf reader: %w", err)
	}
	nodes = append(nodes, leaf)
	ancestor := leaf.Ancestor()
	for (ancestor != nodestore.NodeID{}) {
		leaf, rsc, err := tr.GetLeafNode(ctx, ancestor)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get ancestor: %w", err)
		}
		if err := rsc.Close(); err != nil {
			return nil, nil, fmt.Errorf("failed to close ancestor reader: %w", err)
		}
		nodes = append(nodes, leaf)
		ancestor = leaf.Ancestor()
	}
	slices.Reverse(nodes)
	rangesets := [][][]uint64{}
	start := uint64(0)
	end := uint64(math.MaxUint64)
	for _, leaf := range nodes {
		if leaf.AncestorDeleted() {
			for i, rangeset := range rangesets {
				rangesets[i] = SplitRangeSet(rangeset, leaf.AncestorDeleteStart(), leaf.AncestorDeleteEnd())
			}
		}
		rangesets = append(rangesets, [][]uint64{{start, end}})
	}
	keys := []nodestore.MessageKey{}
	for i, rangeset := range rangesets {
		messageKeys := nodes[i].MessageKeys()
		for _, key := range messageKeys {
			for _, r := range rangeset {
				if key.Timestamp >= r[0] && key.Timestamp < r[1] {
					keys = append(keys, key)
					break
				}
			}
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Timestamp == keys[j].Timestamp {
			return keys[i].Sequence < keys[j].Sequence
		}
		return keys[i].Timestamp < keys[j].Timestamp
	})
	return leaf, keys, nil
}

// nolint: funlen
func mergeLeaves(
	ctx context.Context,
	cw *util.CountingWriter,
	version uint64,
	destVersion *uint64,
	dest *util.Pair[TreeReader, nodestore.NodeID],
	inputs []util.Pair[TreeReader, nodestore.NodeID],
) (
	nodeID nodestore.NodeID,
	stats map[string]*nodestore.Statistics,
	err error,
) {
	if len(inputs) == 0 {
		return nodeID, nil, errors.New("no leaves to merge")
	}

	// If we have no destination and only one input, leaf bytes can be copied
	// without decompression.
	if dest == nil && len(inputs) == 1 {
		nodeID, err := mergeOneLeafDirect(ctx, cw, version, nil, nil, inputs[0])
		if err != nil {
			return nodestore.NodeID{}, nil, fmt.Errorf("failed to merge single node: %w", err)
		}
		return nodeID, nil, nil
	}

	var ancestorNodeID *nodestore.NodeID
	var ancestorVersion *uint64
	var filter []nodestore.MessageKey
	if dest != nil {
		ancestorNodeID = &dest.Second
		ancestorVersion = destVersion
		_, filter, err = buildFilter(ctx, dest.First, dest.Second)
		if err != nil {
			return nodestore.NodeID{}, nil, err
		}
	}

	// Otherwise, we need to merge leaves.
	newHeader := nodestore.NewLeafNode(nil, []byte{}, ancestorNodeID, ancestorVersion)
	iterators := make([]mcap.MessageIterator, len(inputs))

	// Build a list of message keys associated with the incoming data.
	keys := []nodestore.MessageKey{}
	for _, input := range inputs {
		header, err := getHeader(ctx, input)
		if err != nil {
			return nodestore.NodeID{}, nil, err
		}
		if header.AncestorDeleted() {
			if ancestorNodeID == nil {
				return nodestore.NodeID{}, nil, errElideNode
			}
			start := header.AncestorDeleteStart()
			end := header.AncestorDeleteEnd()
			newHeader.DeleteRange(start, end)
			keys = util.Filter(func(key nodestore.MessageKey) bool {
				return key.Timestamp < start || key.Timestamp >= end
			}, keys)
		}
		keys = append(keys, header.MessageKeys()...)
	}

	// If the length of inputs is 1, we can still skip merging if there is no
	// overlap between the filter and the list of keys we just built.
	if len(inputs) == 1 && !util.HashSliceOverlap(filter, keys) {
		nodeID, err := mergeOneLeafDirect(
			ctx, cw, version, ancestorNodeID, ancestorVersion, inputs[0])
		if err != nil {
			return nodestore.NodeID{}, nil, fmt.Errorf("failed to merge single node: %w", err)
		}
		return nodeID, nil, nil
	}

	// Otherwise, we will need to merge bytes. Take another pass through the
	// inputs to get the iterators.
	for i, input := range inputs {
		iterator, finish, err := getIterator(ctx, input)
		if err != nil {
			return nodestore.NodeID{}, nil, err
		}
		defer util.MaybeWarn(ctx, finish)
		iterators[i] = iterator
	}
	newHeader.SetMessageKeys(keys)
	schemaStats, callback := onMessageCallback()
	offset := uint64(cw.Count())
	if err := newHeader.EncodeTo(cw); err != nil {
		return nodestore.NodeID{}, nil, fmt.Errorf("failed to write leaf header: %w", err)
	}
	if err = mcap.FilterMerge(cw, callback, filter, iterators...); err != nil {
		return nodestore.NodeID{}, nil, fmt.Errorf("failed to merge leaf iterators: %w", err)
	}
	length := uint64(cw.Count()) - offset
	nodeID = nodestore.NewNodeID(version, offset, length)
	return nodeID, schemaStats, nil
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

func getInnerNodes(
	ctx context.Context,
	inputs []util.Pair[TreeReader, nodestore.NodeID],
) ([]*nodestore.InnerNode, error) {
	nodes := make([]*nodestore.InnerNode, 0, len(inputs))
	for i, input := range inputs {
		node, err := toNode[*nodestore.InnerNode](ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to parse node %d: %w", i, err)
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// merge inner nodes constructs a new inner node with "merges" in the location
// of any "conflicts" among the children of the inputs.
func mergeInnerNode( // nolint: funlen
	ctx context.Context,
	cw *util.CountingWriter,
	version uint64,
	dest *util.Pair[TreeReader, nodestore.NodeID],
	inputs []util.Pair[TreeReader, nodestore.NodeID],
) (nodeID nodestore.NodeID, stats map[string]*nodestore.Statistics, err error) {
	if len(inputs) == 0 {
		return nodeID, nil, errors.New("no children to merge")
	}
	innerNodes, err := getInnerNodes(ctx, inputs)
	if err != nil {
		return nodeID, nil, err
	}

	// reference head to set dimensions of the output
	head := innerNodes[0]

	// Create a new inner node with same dimensions as the reference. In the
	// location of each conflict, compute a node ID via a recursive merge and
	// form a child structure from the aggregated statistics of the conflicted
	// children.
	newNode := nodestore.NewInnerNode(head.Height, head.Start, head.End, len(head.Children))

	// default the children of the new node, to those of the destination node,
	// if one exists.
	var destNode *nodestore.InnerNode
	if dest != nil {
		destNode, err = toNode[*nodestore.InnerNode](ctx, *dest)
		if err != nil {
			return nodestore.NodeID{}, stats, err
		}
		newNode.Children = destNode.Children
	}

	isSingleton := len(inputs) == 1

	// gather child indexes where conflicts occur
	conflicts := []int{}
	for i, child := range head.Children {
		// For a singleton node, all children must be treated as conflicts.
		if child != nil && isSingleton {
			conflicts = append(conflicts, i)
			continue
		}
		if slices.IndexFunc(innerNodes[1:], isConflicted(child, i)) > -1 {
			conflicts = append(conflicts, i)
			continue
		}
	}

	outputStats := make(map[string]*nodestore.Statistics)
	tmp := slices.Clone(innerNodes)

	// build a merged child in the location of each conflict.
	for _, conflict := range conflicts {
		// sort the nodes by the version at the conflict location. This is used
		// for the tombstone handling below, to blank statistics/history when a
		// tombstone is encountered.
		sort.Slice(inputs, order(tmp, conflict))
		sort.Slice(innerNodes, order(tmp, conflict))

		var destChild *util.Pair[TreeReader, nodestore.NodeID]
		var destVersion *uint64
		statistics := map[string]*nodestore.Statistics{}
		if destNode != nil && destNode.Children[conflict] != nil {
			destChild = util.Pointer(util.NewPair(dest.First, destNode.Children[conflict].ID))
			destVersion = &destNode.Children[conflict].Version
			statistics = nodestore.CloneStatsMap(destNode.Children[conflict].Statistics)
		}

		_, conflictedNodes, conflictedPairs := buildConflictedPairs(
			conflict, inputs, innerNodes, statistics)
		// If we are in the fast path we have no destination and only one
		// input, and require no merging of leaves. In this case we copy
		// leaf bytes directly and do not compute statistics at merge time.

		// now refine the merging strategy to include message indexes

		var targetChild *nodestore.Child
		if destChild != nil {
			targetChild = destNode.Children[conflict]
		}

		if len(conflictedPairs) > 0 { // nolint: nestif
			var destConflict *nodestore.Child
			if destChild != nil {
				destConflict = destNode.Children[conflict]
			}
			mergedID, mergedStats, err := mergeConflictedPairs(
				ctx, newNode, cw, version, destVersion, destChild, destConflict, conflictedPairs,
			)
			if err != nil {
				if errors.Is(err, errElideNode) {
					continue
				}
				return nodestore.NodeID{}, nil, fmt.Errorf("failed to merge nodes: %w", err)
			}

			// Signifies we merged one leaf efficiently and need to merge the
			// destination stats with the single input's children.
			if mergedStats == nil {
				mergedStats = nodestore.CloneStatsMap(conflictedNodes[0].Children[conflict].Statistics)
				if targetChild != nil {
					mergedStats, err = nodestore.MergeStatsMaps(mergedStats, targetChild.Statistics)
					if err != nil {
						return nodestore.NodeID{}, nil, fmt.Errorf("failed to merge stats: %w", err)
					}
				}
			}

			newNode.Children[conflict] = &nodestore.Child{
				ID:         mergedID,
				Version:    version,
				Statistics: mergedStats,
			}
			continue
		}
		// If the final input was a tombstone, we need to blank any
		// inherited from the destination. However, we still include the
		// statistics. This is inaccurate behavior that we may want to
		// revise at some point, but revising it requires retraversal of
		// underlying data.
		if newNode.Children[conflict] != nil {
			stats := newNode.Children[conflict].Statistics
			outputStats, err = nodestore.MergeStatsMaps(outputStats, stats)
			if err != nil {
				return nodestore.NodeID{}, nil, fmt.Errorf("failed to merge stats: %w", err)
			}
			newNode.Children[conflict] = nil
		}
	}

	for _, child := range newNode.Children {
		if child == nil {
			continue
		}
		outputStats, err = nodestore.MergeStatsMaps(outputStats, child.Statistics)
		if err != nil {
			return nodestore.NodeID{}, nil, fmt.Errorf("failed to merge stats: %w", err)
		}
	}

	// Now serialize the new inner node.
	data := newNode.ToBytes()
	offset := uint64(cw.Count())
	_, err = cw.Write(data)
	if err != nil {
		return nodestore.NodeID{}, nil, fmt.Errorf("failed to serialize inner node: %w", err)
	}

	return nodestore.NewNodeID(version, offset, uint64(len(data))), outputStats, nil
}

func mergeOneLeafDirect(
	ctx context.Context,
	cw *util.CountingWriter,
	version uint64,
	ancestorID *nodestore.NodeID,
	ancestorVersion *uint64,
	pair util.Pair[TreeReader, nodestore.NodeID],
) (nodestore.NodeID, error) {
	tr := pair.First
	nodeID := pair.Second
	header, r, err := tr.GetLeafNode(ctx, nodeID)
	if err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to get leaf: %w", err)
	}
	defer r.Close()
	newHeader := nodestore.NewLeafNode(
		header.MessageKeys(),
		[]byte{},
		ancestorID,
		ancestorVersion,
	)
	if header.AncestorDeleted() {
		newHeader.DeleteRange(header.AncestorDeleteStart(), header.AncestorDeleteEnd())
	}
	offset := uint64(cw.Count())
	if err := newHeader.EncodeTo(cw); err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to write leaf header: %w", err)
	}
	if _, err := io.Copy(cw, r); err != nil {
		return nodestore.NodeID{}, fmt.Errorf("failed to copy leaf data: %w", err)
	}
	length := uint64(cw.Count()) - offset

	return nodestore.NewNodeID(version, offset, length), nil
}

func mergeConflictedPairs(
	ctx context.Context,
	newNode *nodestore.InnerNode,
	cw *util.CountingWriter,
	version uint64,
	destVersion *uint64,
	dest *util.Pair[TreeReader, nodestore.NodeID],
	destChild *nodestore.Child,
	conflictedPairs []util.Pair[TreeReader, nodestore.NodeID],
) (nodestore.NodeID, map[string]*nodestore.Statistics, error) {
	switch {
	case newNode.Height > 1:
		return mergeInnerNode(ctx, cw, version, dest, conflictedPairs)
	case destChild == nil && len(conflictedPairs) == 1:
		nodeID, err := mergeOneLeafDirect(ctx, cw, version, nil, nil, conflictedPairs[0])
		if err != nil {
			return nodestore.NodeID{}, nil, fmt.Errorf("failed to merge single node: %w", err)
		}
		return nodeID, nil, nil
	default:
		mergedID, mergedStats, err := mergeLeaves(
			ctx, cw, version, destVersion, dest, conflictedPairs,
		)
		if err != nil {
			return nodestore.NodeID{}, nil, fmt.Errorf("failed to merge nodes: %w", err)
		}

		if destChild != nil && mergedStats != nil {
			mergedStats, err = nodestore.MergeStatsMaps(mergedStats, destChild.Statistics)
			if err != nil {
				return nodestore.NodeID{}, nil, fmt.Errorf("failed to merge stats: %w", err)
			}
		}

		return mergedID, mergedStats, nil
	}
}

func buildConflictedPairs(
	conflict int,
	inputs []util.Pair[TreeReader, nodestore.NodeID],
	nodes []*nodestore.InnerNode,
	statistics map[string]*nodestore.Statistics,
) (uint64, []*nodestore.InnerNode, []util.Pair[TreeReader, nodestore.NodeID]) {
	conflictedPairs := []util.Pair[TreeReader, nodestore.NodeID]{}
	conflictedNodes := []*nodestore.InnerNode{}
	var maxVersion uint64
	for i, pair := range inputs {
		node := nodes[i]
		child := node.Children[conflict]
		if child == nil {
			continue
		}
		maxVersion = max(maxVersion, child.Version)

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
		conflictedNodes = append(conflictedNodes, node)
	}
	return maxVersion, conflictedNodes, conflictedPairs
}

func isConflicted(child *nodestore.Child, i int) func(*nodestore.InnerNode) bool {
	return func(sibling *nodestore.InnerNode) bool {
		cousin := sibling.Children[i]
		return child == nil && cousin != nil ||
			child != nil && cousin == nil ||
			child != nil && cousin != nil
	}
}

func order(nodes []*nodestore.InnerNode, conflict int) func(i, j int) bool {
	return func(i, j int) bool {
		if nodes[i].Children[conflict] == nil || nodes[j].Children[conflict] == nil {
			return false
		}
		return nodes[i].Children[conflict].Version < nodes[j].Children[conflict].Version
	}
}
