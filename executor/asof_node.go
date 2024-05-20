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

// asofJoinNode represents an as-of join.
type asofJoinNode struct {
	pq *util.PriorityQueue[queueElement]

	children []Node

	lastLeft  *tuple
	lastRight *tuple

	immediate   bool
	leftEmitted bool

	threshold   uint64
	initialized bool
}

// Close the node.
func (n *asofJoinNode) Close(ctx context.Context) error {
	errs := make([]error, 0, len(n.children))
	for _, child := range n.children {
		if err := child.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to close children: %w", errs[0])
	}
	return nil
}

func (n *asofJoinNode) initialize(ctx context.Context) error {
	for i := range n.children {
		next, err := n.children[i].Next(ctx)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("failed to get next message on left child: %w", err)
		}
		if next != nil {
			heap.Push(n.pq, queueElement{tuple: next, index: i})
		}
	}
	n.initialized = true
	return nil
}

// Next gets the next tuple from the node, or returns io.EOF if no tuple exists.
func (n *asofJoinNode) Next(ctx context.Context) (*tuple, error) {
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
			heap.Push(n.pq, queueElement{tuple: next, index: element.index})
		}
		if element.index == 0 {
			n.lastLeft = element.tuple
			n.leftEmitted = false
			continue
		}
		if n.lastLeft == nil {
			continue
		}

		if n.lastLeft.message.LogTime+n.threshold > element.tuple.message.LogTime || n.threshold == 0 {
			// if we're not in immediate mode, and we already emitted the left
			// tuple, emit right tuples.
			if !n.leftEmitted {
				n.lastRight = element.tuple
				n.leftEmitted = true
				return n.lastLeft, nil
			}
			if n.immediate {
				continue
			}
			return element.tuple, nil
		}
	}
	return nil, io.EOF
}

// String returns the string representation of the node.
func (n *asofJoinNode) String() string {
	return fmt.Sprintf(
		"[asof %d %s %s %s]",
		n.threshold,
		util.When(n.immediate, "immediate", "full"),
		n.children[0].String(), n.children[1].String(),
	)
}

// NewAsofJoinNode constructs a new as-of join node.
func NewAsofJoinNode(left, right Node, immediate bool, threshold uint64) *asofJoinNode {
	children := []Node{left, right}
	pq := util.NewPriorityQueue[queueElement](func(a, b queueElement) bool {
		if a.tuple.message.LogTime == b.tuple.message.LogTime {
			return a.tuple.message.ChannelID < b.tuple.message.ChannelID
		}
		return a.tuple.message.LogTime < b.tuple.message.LogTime
	})
	return &asofJoinNode{children: children, threshold: threshold, immediate: immediate, pq: pq}
}
