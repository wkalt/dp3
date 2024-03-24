package tree

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
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

	leafIDs     []nodestore.NodeID
	nextLeaf    int
	reader      *mcap.Reader
	rsc         io.ReadSeekCloser
	msgIterator mcap.MessageIterator
	tr          TreeReader
}

// NewTreeIterator returns a new iterator over the given tree.
func NewTreeIterator(
	ctx context.Context,
	tr TreeReader,
	start uint64,
	end uint64,
) (*Iterator, error) {
	it := &Iterator{start: start, end: end, tr: tr}
	if err := it.initialize(ctx, tr.Root()); err != nil {
		return nil, err
	}
	return it, nil
}

// Close closes the iterator if it has not already been exhausted.
func (ti *Iterator) Close() error {
	if ti.reader != nil {
		ti.reader.Close()
	}
	if ti.rsc != nil {
		if err := ti.rsc.Close(); err != nil {
			return fmt.Errorf("failed to close reader: %w", err)
		}
	}
	return nil
}

// More returns true if there are more elements in the iteration.
func (ti *Iterator) More() bool {
	return ti.nextLeaf < len(ti.leafIDs)
}

// Next returns the next element in the iteration.
func (ti *Iterator) Next(ctx context.Context) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	for ti.nextLeaf < len(ti.leafIDs) {
		if ti.msgIterator == nil {
			if err := ti.openNextLeaf(ctx); err != nil {
				return nil, nil, nil, err
			}
		}
		schema, channel, message, err := ti.msgIterator.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				ti.reader.Close()
				if err := ti.rsc.Close(); err != nil {
					return nil, nil, nil, fmt.Errorf("failed to close leaf reader: %w", err)
				}
				ti.reader = nil
				ti.rsc = nil
				ti.msgIterator = nil
				ti.nextLeaf++
				continue
			}
			return nil, nil, nil, fmt.Errorf("failed to read message: %w", err)
		}
		return schema, channel, message, nil
	}
	return nil, nil, nil, io.EOF
}

// Initializing an iterator at a root ID will populate the iterator's list of
// leaf IDs with those that overlap with the configured request parameters.
func (ti *Iterator) initialize(ctx context.Context, rootID nodestore.NodeID) error {
	stack := []nodestore.NodeID{rootID}
	for len(stack) > 0 {
		nodeID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node, err := ti.tr.Get(ctx, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get node %s: %w", nodeID, err)
		}
		if node.Type() == nodestore.Leaf {
			ti.leafIDs = append(ti.leafIDs, nodeID)
			continue
		}
		inner, ok := node.(*nodestore.InnerNode)
		if !ok {
			return errors.New("expected inner node - tree is corrupt")
		}
		step := bwidth(inner)
		left := inner.Start
		right := inner.Start + step
		for _, child := range inner.Children {
			if child != nil {
				if ti.start < right*1e9 && ti.end >= left*1e9 {
					stack = append(stack, child.ID)
				}
			}
			left += step
			right += step
		}
	}
	return nil
}

// openNextLeaf opens the next leaf in the iterator.
func (ti *Iterator) openNextLeaf(ctx context.Context) error {
	leafID := ti.leafIDs[ti.nextLeaf]
	rsc, err := ti.tr.GetLeafData(ctx, leafID)
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}
	reader, err := mcap.NewReader(rsc)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	it, err := reader.Messages(mcap.AfterNanos(ti.start), mcap.BeforeNanos(ti.end))
	if err != nil {
		return fmt.Errorf("failed to create message iterator: %w", err)
	}
	ti.msgIterator = it
	ti.reader = reader
	ti.rsc = rsc
	return nil
}
