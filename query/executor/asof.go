package executor

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/wkalt/dp3/util"
)

/*
In an as-of join, two tables are related based on a time delay rather than based
on an equijoin condition. In our implementation, assuming a join of tables A and
B with threshold t, a tuple from A is returned if it is succeeded within t
nanoseconds by a tuple on B.

If this occurs, the tuples from both A and B are returned. If not, neither is
returned.
*/

////////////////////////////////////////////////////////////////////////////////

// AsofJoinNode represents an as-of join.
type AsofJoinNode struct {
	pq *util.PriorityQueue[queueElement, uint64]

	children []Node

	lastLeft  *Tuple
	lastRight *Tuple

	immediate bool

	threshold   uint64
	initialized bool
}

// Close the node.
func (n *AsofJoinNode) Close() error {
	errs := make([]error, 0, len(n.children))
	for _, child := range n.children {
		if err := child.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to close children: %w", errs[0])
	}
	return nil
}

func (n *AsofJoinNode) initialize(ctx context.Context) error {
	for i := range n.children {
		next, err := n.children[i].Next(ctx)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("failed to get next message on left child: %w", err)
		}
		if next != nil {
			item := util.Item[queueElement, uint64]{
				Value:    queueElement{tuple: next, index: i},
				Priority: next.Message.LogTime,
			}
			heap.Push(n.pq, &item)
		}
	}
	n.initialized = true
	return nil
}

// Next gets the next tuple from the node, or returns io.EOF if no tuple exists.
func (n *AsofJoinNode) Next(ctx context.Context) (*Tuple, error) {
	if !n.initialized {
		if err := n.initialize(ctx); err != nil {
			return nil, fmt.Errorf("failed to initialize asof join node: %w", err)
		}
	}
	if msg := n.lastRight; msg != nil {
		n.lastRight = nil
		return msg, nil
	}
	for n.pq.Len() > 0 {
		element := heap.Pop(n.pq).(queueElement)
		next, err := n.children[element.index].Next(ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("failed to get next tuple: %w", err)
			}
		} else {
			item := util.Item[queueElement, uint64]{
				Value:    queueElement{tuple: next, index: element.index},
				Priority: next.Message.LogTime,
			}
			heap.Push(n.pq, &item)
		}
		if element.index == 0 {
			n.lastLeft = element.tuple
			continue
		}
		if n.lastLeft == nil {
			continue
		}
		if n.lastLeft.Message.LogTime+n.threshold > element.tuple.Message.LogTime || n.threshold == 0 {
			output := n.lastLeft
			n.lastRight = element.tuple
			n.lastLeft = nil
			return output, nil
		}
	}
	return nil, io.EOF
}

// String returns the string representation of the node.
func (n *AsofJoinNode) String() string {
	return fmt.Sprintf(
		"[asof %d %s %s %s]",
		n.threshold,
		util.When(n.immediate, "immediate", "full"),
		n.children[0].String(), n.children[1].String(),
	)
}

// NewAsofJoinNode constructs a new as-of join node.
func NewAsofJoinNode(left, right Node, immediate bool, threshold uint64) *AsofJoinNode {
	children := []Node{left, right}
	pq := util.NewPriorityQueue[queueElement, uint64]()
	return &AsofJoinNode{children: children, threshold: threshold, immediate: immediate, pq: pq}
}
