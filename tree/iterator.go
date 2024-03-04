package tree

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/nodestore"
)

// Iterator is an iterator over a tree.
type Iterator struct {
	start uint64
	end   uint64

	leafIDs     []nodestore.NodeID
	nextLeaf    int
	reader      *mcap.Reader
	msgIterator mcap.MessageIterator
	ns          *nodestore.Nodestore
}

func (ti *Iterator) initialize(ctx context.Context, rootID nodestore.NodeID) error {
	var stack []nodestore.NodeID
	stack = append(stack, rootID)
	for len(stack) > 0 {
		nodeID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node, err := ti.ns.Get(ctx, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get node: %w", err)
		}
		if node.Type() == nodestore.Leaf {
			ti.leafIDs = append(ti.leafIDs, nodeID)
			continue
		}
		inner, ok := node.(*nodestore.InnerNode)
		if !ok {
			return errors.New("expected inner node - tree is corrupt")
		}
		span := inner.End - inner.Start
		step := span / uint64(len(inner.Children))
		left := inner.Start
		right := inner.Start + step
		for _, child := range inner.Children {
			if left < ti.end && right >= ti.start {
				stack = append(stack, child.ID)
			}
			left += step
			right += step
		}
	}
	return nil
}

func (ti *Iterator) openNextLeaf(ctx context.Context) error {
	leafID := ti.leafIDs[ti.nextLeaf]
	node, err := ti.ns.Get(ctx, leafID)
	if err != nil {
		return fmt.Errorf("failed to get node: %w", err)
	}
	leaf, ok := node.(*nodestore.LeafNode)
	if !ok {
		return errors.New("expected leaf node - tree is corrupt")
	}
	reader, err := mcap.NewReader(leaf.Data())
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	it, err := reader.Messages(mcap.After(ti.start), mcap.Before(ti.end))
	if err != nil {
		return fmt.Errorf("failed to create message iterator: %w", err)
	}
	ti.msgIterator = it
	ti.reader = reader
	return nil
}

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
				ti.reader = nil
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

// NewTreeIterator returns a new iterator over the given tree.
func NewTreeIterator(
	ctx context.Context,
	ns *nodestore.Nodestore,
	rootID nodestore.NodeID,
	version, start uint64,
	end uint64,
) (*Iterator, error) {
	it := &Iterator{start: start, end: end, ns: ns}
	if err := it.initialize(ctx, rootID); err != nil {
		return nil, err
	}
	return it, nil
}
