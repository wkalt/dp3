package tree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/nodestore"
	"github.com/wkalt/dp3/server/util"
	"github.com/wkalt/dp3/server/util/log"
)

/*
The tree iterator holds the state of an active scan over leaves of a tree,
primarily related to opening and closing leaf MCAP data.
*/

////////////////////////////////////////////////////////////////////////////////

// Iterator is an iterator over a tree.
type Iterator struct {
	descending bool
	start      uint64
	end        uint64

	readclosers []io.ReadSeekCloser
	msgIterator mcap.MessageIterator
	tr          Reader
	minVersion  uint64

	childFilter func(*nodestore.Child) (bool, error)

	queue []nodestore.NodeID

	stats IterationStats
}

// NewTreeIterator returns a new iterator over the given tree.
func NewTreeIterator(
	tr Reader,
	descending bool,
	start uint64,
	end uint64,
	minVersion uint64,
	childFilter func(*nodestore.Child) (bool, error),
) *Iterator {
	it := &Iterator{
		descending:  descending,
		start:       start,
		end:         end,
		tr:          tr,
		minVersion:  minVersion,
		childFilter: childFilter,
	}
	it.queue = []nodestore.NodeID{tr.Root()}
	return it
}

type IterationStats struct {
	InnerNodesScanned  uint64
	InnerNodesFiltered uint64
	LeafNodesScanned   uint64
	LeafNodesFiltered  uint64
}

// Close closes the iterator if it has not already been exhausted.
func (ti *Iterator) Close(ctx context.Context) error {
	if err := ti.closeActiveReaders(); err != nil {
		return fmt.Errorf("failed to close leaf reader: %w", err)
	}
	ctx, _ = util.WithChildContext(ctx, "scan statistics")
	innerNodesFiltered := strconv.FormatUint(ti.stats.InnerNodesFiltered, 10)
	innerNodesScanned := strconv.FormatUint(ti.stats.InnerNodesScanned, 10)
	leafNodesFiltered := strconv.FormatUint(ti.stats.LeafNodesFiltered, 10)
	leafNodesScanned := strconv.FormatUint(ti.stats.LeafNodesScanned, 10)
	util.SetContextData(ctx, "inner_nodes_filtered", innerNodesFiltered)
	util.SetContextData(ctx, "inner_nodes_scanned", innerNodesScanned)
	util.SetContextData(ctx, "leaf_nodes_filtered", leafNodesFiltered)
	util.SetContextData(ctx, "leaf_nodes_scanned", leafNodesScanned)

	return nil
}

// More returns true if there are more elements in the iteration.
func (ti *Iterator) More() bool {
	return len(ti.readclosers) > 0 || len(ti.queue) > 0
}

func (ti *Iterator) closeActiveReaders() error {
	errs := make([]error, 0, len(ti.readclosers))
	for _, closer := range ti.readclosers {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to close %d closers: %v", len(errs), errs)
	}
	return nil
}

// advance moves the iterator to the next leaf. If there are no more leaves, it
// will return a wrapped io.EOF.
func (ti *Iterator) advance(ctx context.Context) error {
	if err := ti.closeActiveReaders(); err != nil {
		return fmt.Errorf("failed to close leaf reader: %w", err)
	}
	ti.readclosers = nil
	ti.msgIterator = nil
	if err := ti.openNextLeaf(ctx); err != nil {
		return fmt.Errorf("failed to open next leaf: %w", err)
	}
	return nil
}

// Next returns the next element in the iteration.
func (ti *Iterator) Next(ctx context.Context) (*fmcap.Schema, *fmcap.Channel, *fmcap.Message, error) {
	for {
		// first call sets the state
		if ti.msgIterator == nil {
			if err := ti.openNextLeaf(ctx); err != nil {
				return nil, nil, nil, err
			}
		}
		schema, channel, message, err := ti.msgIterator.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if err := ti.advance(ctx); err != nil {
					return nil, nil, nil, err
				}
				continue
			}
			return nil, nil, nil, fmt.Errorf("failed to read message: %w", err)
		}
		return schema, channel, message, nil
	}
}

// getNextLeaf returns the next leaf node ID in the iteration and advances the
// internal node stack. The descending flag determines the direction of the
// iteration.
func (ti *Iterator) getNextLeaf(ctx context.Context) (nodeID nodestore.NodeID, err error) {
	for len(ti.queue) > 0 {
		nodeID := ti.queue[0]
		ti.queue = ti.queue[1:]
		node, err := ti.tr.Get(ctx, nodeID)
		if err != nil {
			return nodeID, fmt.Errorf("failed to get node %s: %w", nodeID, err)
		}
		if node.Type() == nodestore.Leaf {
			return nodeID, nil
		}
		inner, ok := node.(*nodestore.InnerNode)
		if !ok {
			return nodeID, errors.New("expected inner node - tree is corrupt")
		}
		step := bwidth(inner)
		left := inner.Start
		right := inner.Start + step
		children := slices.Clone(inner.Children) // to avoid mutating the cached children
		if ti.descending {
			left = inner.End - step
			right = inner.End
			step = -step
			slices.Reverse(children)
		}
		for _, child := range children {
			ok := child != nil
			ok = ok && child.Version > ti.minVersion
			ok = ok && ti.start < right*1e9 && ti.end >= left*1e9
			if ok && ti.childFilter != nil {
				ok, err = ti.childFilter(child)
				if err != nil {
					return nodeID, fmt.Errorf("failed to filter child: %w", err)
				}
				if !ok {
					ti.stats.InnerNodesFiltered += uint64(util.Pow(64, int(inner.Height)-1))
					ti.stats.LeafNodesFiltered += uint64(util.Pow(64, int(inner.Height)))
					log.Debugf(ctx, "skipping node due to filter")
				}
			}

			if ok {
				if inner.Height > 1 {
					ti.stats.InnerNodesScanned++
				} else {
					ti.stats.LeafNodesScanned++
				}
				ti.queue = append(ti.queue, child.ID)
			}
			left += step
			right += step
		}
	}
	return nodeID, io.EOF
}

// BuildLeafIterator returns an mcap.MessageIterator over a leaf node,
// accounting for ancestors.
func BuildLeafIterator(
	ctx context.Context,
	tr Reader,
	leafID nodestore.NodeID,
	descending bool,
) (mcap.MessageIterator, func() error, error) {
	nodes := []*nodestore.LeafNode{}
	readers := []io.ReadSeekCloser{}
	leaf, rsc, err := tr.GetLeafNode(ctx, leafID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get leaf: %w", err)
	}
	nodes = append(nodes, leaf)
	readers = append(readers, rsc)

	ancestor := leaf.Ancestor()
	for (ancestor != nodestore.NodeID{}) {
		leaf, rsc, err := tr.GetLeafNode(ctx, ancestor)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get ancestor: %w", err)
		}
		nodes = append(nodes, leaf)
		readers = append(readers, rsc)
		ancestor = leaf.Ancestor()
	}
	// now we have the full list of ancestors, and readers, in reverse
	// chronological order. Most likely (technically up to the storage
	// implementation) no IO has been performed. Now go forward through the list
	// to figure out what time ranges we need to grab from each element.
	slices.Reverse(nodes)
	slices.Reverse(readers)
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
	iterators := make([]mcap.MessageIterator, len(rangesets))
	for i := range rangesets {
		iterators[i] = mcap.NewConcatIterator(readers[i], rangesets[i], descending)
	}
	finish := func() error {
		if err := util.CloseAll(readers...); err != nil {
			return fmt.Errorf("failed to close all readers: %w", err)
		}
		return nil
	}
	if len(iterators) == 1 {
		return iterators[0], finish, nil
	}
	it, err := mcap.NmergeIterator(descending, iterators...)
	if err != nil {
		return nil, finish, fmt.Errorf("failed to merge iterators: %w", err)
	}
	return it, finish, nil
}

// openNextLeaf opens the next iterator to scan. A leaf may be a single node, or
// it may be the tail of a linked list, pointing backward at one or more
// ancestor nodes. This occurs either when new data is inserted over an existing
// leaf, or when data from a leaf is deleted. We need to take care to ensure we
// handle both situations correctly, and also efficiently.
//
//	Example: w(1, 5) -> w(6, 10) -> d(4, 8) ->  w(7, 15)
//
// 1. Create a [][][]uint64
// 2. Traverse all the way back to the start of the list
// 3. Add the first element: {{{1, 5}}}
// 4. Add the second element: {{{1, 5}}, {{6, 10}}}
// 5. Split previous ranges, possibly subdividing them: {{{1, 4}}, {{8, 10}}}
// 6. Add a new element: {{{1, 4}}, {{8, 10}}, {{7, 15}}}. Merge these.
func (ti *Iterator) openNextLeaf(ctx context.Context) error {
	leafID, err := ti.getNextLeaf(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next leaf: %w", err)
	}
	// merge iterator of mcap iterators, or just the singleton.
	nodes := []*nodestore.LeafNode{}
	readers := []io.ReadSeekCloser{}

	leaf, rsc, err := ti.tr.GetLeafNode(ctx, leafID)
	if err != nil {
		return fmt.Errorf("failed to get leaf: %w", err)
	}
	nodes = append(nodes, leaf)
	readers = append(readers, rsc)

	ancestor := leaf.Ancestor()
	for (ancestor != nodestore.NodeID{}) {
		leaf, rsc, err := ti.tr.GetLeafNode(ctx, ancestor)
		if err != nil {
			return fmt.Errorf("failed to get ancestor: %w", err)
		}
		nodes = append(nodes, leaf)
		readers = append(readers, rsc)
		ancestor = leaf.Ancestor()
	}
	// now we have the full list of ancestors, and readers, in reverse
	// chronological order. Most likely (technically up to the storage
	// implementation) no IO has been performed. Now go forward through the list
	// to figure out what time ranges we need to grab from each element.
	slices.Reverse(nodes)
	slices.Reverse(readers)
	rangesets := [][][]uint64{}
	for _, leaf := range nodes {
		if leaf.AncestorDeleted() {
			for i, rangeset := range rangesets {
				rangesets[i] = SplitRangeSet(rangeset, leaf.AncestorDeleteStart(), leaf.AncestorDeleteEnd())
			}
		}
		rangesets = append(rangesets, [][]uint64{{ti.start, ti.end}})
	}
	iterators := make([]mcap.MessageIterator, len(rangesets))
	for i := range rangesets {
		iterators[i] = mcap.NewConcatIterator(readers[i], rangesets[i], ti.descending)
	}
	ti.readclosers = readers
	if len(iterators) == 1 {
		ti.msgIterator = iterators[0]
	} else {
		ti.msgIterator, err = mcap.NmergeIterator(ti.descending, iterators...)
		if err != nil {
			return fmt.Errorf("failed to merge iterators: %w", err)
		}
	}
	return nil
}

func SplitRangeSet(rangeset [][]uint64, left uint64, right uint64) [][]uint64 {
	newRangeset := [][]uint64{}
	for _, r := range rangeset {
		if r[1] < left || r[0] > right {
			newRangeset = append(newRangeset, r)
			continue
		}
		if r[0] < left {
			newRangeset = append(newRangeset, []uint64{r[0], left})
		}
		if r[1] > right {
			newRangeset = append(newRangeset, []uint64{right, r[1]})
		}
	}
	return newRangeset
}
