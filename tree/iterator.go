package tree

import (
	"context"
	"errors"
	"fmt"
	"io"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
)

/*
The tree iterator holds the state of an active scan over leaves of a tree,
primarily related to opening and closing leaf MCAP data.
*/

////////////////////////////////////////////////////////////////////////////////

// Iterator is an iterator over a tree.
type Iterator struct {
	start uint64
	end   uint64

	readers     []*fmcap.Reader
	closers     []io.Closer
	msgIterator fmcap.MessageIterator
	tr          TreeReader
	minVersion  uint64

	stack []nodestore.NodeID
}

// NewTreeIterator returns a new iterator over the given tree.
func NewTreeIterator(
	ctx context.Context,
	tr TreeReader,
	start uint64,
	end uint64,
	minVersion uint64,
) *Iterator {
	it := &Iterator{start: start, end: end, tr: tr, minVersion: minVersion}
	it.stack = []nodestore.NodeID{tr.Root()}
	return it
}

// Close closes the iterator if it has not already been exhausted.
func (ti *Iterator) Close() error {
	return ti.closeActiveReaders()
}

// More returns true if there are more elements in the iteration.
func (ti *Iterator) More() bool {
	return len(ti.readers) > 0 || len(ti.stack) > 0
}

func (ti *Iterator) closeActiveReaders() error {
	for _, reader := range ti.readers {
		reader.Close()
	}
	errs := make([]error, 0, len(ti.closers))
	for _, closer := range ti.closers {
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
	ti.readers = nil
	ti.closers = nil
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
// internal node stack.
func (ti *Iterator) getNextLeaf(ctx context.Context) (nodeID nodestore.NodeID, err error) {
	for len(ti.stack) > 0 {
		nodeID := ti.stack[len(ti.stack)-1]
		ti.stack = ti.stack[:len(ti.stack)-1]
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
		for _, child := range inner.Children {
			if child != nil && child.Version > ti.minVersion {
				if ti.start < right*1e9 && ti.end >= left*1e9 {
					ti.stack = append(ti.stack, child.ID)
				}
			}
			left += step
			right += step
		}
	}
	return nodeID, io.EOF
}

func (ti *Iterator) openLeaf(ctx context.Context, nodeID nodestore.NodeID) (
	nodestore.NodeID, fmcap.MessageIterator, error,
) {
	ancestor, rsc, err := ti.tr.GetLeafData(ctx, nodeID)
	if err != nil {
		return ancestor, nil, fmt.Errorf("failed to get reader: %w", err)
	}
	reader, err := mcap.NewReader(rsc)
	if err != nil {
		return ancestor, nil, fmt.Errorf("failed to create reader: %w", err)
	}
	it, err := reader.Messages(fmcap.AfterNanos(ti.start), fmcap.BeforeNanos(ti.end))
	if err != nil {
		return ancestor, nil, fmt.Errorf("failed to create message iterator: %w", err)
	}
	ti.readers = append(ti.readers, reader)
	ti.closers = append(ti.closers, rsc)
	return ancestor, it, nil
}

// openNextLeaf opens the next leaf in the iterator.
func (ti *Iterator) openNextLeaf(ctx context.Context) error {
	leafID, err := ti.getNextLeaf(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next leaf: %w", err)
	}
	// merge iterator of mcap iterators, or just the singleton.
	ancestor, it, err := ti.openLeaf(ctx, leafID)
	if err != nil {
		return fmt.Errorf("failed to open leaf: %w", err)
	}
	iterators := []fmcap.MessageIterator{it}
	for (ancestor != nodestore.NodeID{}) {
		ancestor, it, err = ti.openLeaf(ctx, ancestor)
		if err != nil {
			return fmt.Errorf("failed to get ancestor: %w", err)
		}
		iterators = append(iterators, it)
	}
	if len(iterators) == 1 {
		ti.msgIterator = iterators[0]
	} else {
		ti.msgIterator, err = mcap.NmergeIterator(iterators...)
		if err != nil {
			return fmt.Errorf("failed to merge iterators: %w", err)
		}
	}
	return nil
}
