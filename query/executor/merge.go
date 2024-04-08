package executor

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/wkalt/dp3/util"
	"golang.org/x/sync/errgroup"
)

/*
MergeNode implements an n-ary time-ordered streaming merge using a heap-based
priority queue. The queue contains at most one element from each child at a
time. When an element is popped from the queue, a new element is pushed from the
child that originated the popped tuple, if available.
*/

////////////////////////////////////////////////////////////////////////////////

type queueElement struct {
	tuple *Tuple
	index int
}

// MergeNode represents the merge node.
type MergeNode struct {
	children []Node
	pq       *util.PriorityQueue[queueElement, uint64]

	initialized bool
}

// NewMergeNode returns a new merge node.
func NewMergeNode(children ...Node) *MergeNode {
	return &MergeNode{
		children:    children,
		pq:          util.NewPriorityQueue[queueElement, uint64](),
		initialized: false,
	}
}

// initialize pushes one message from each child into the priority queue,
// concurrently.
func (n *MergeNode) initialize(ctx context.Context) error {
	g := errgroup.Group{}
	for i, child := range n.children {
		g.Go(func() error {
			tuple, err := child.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return fmt.Errorf("failed to get next message on child %d: %w", i, err)
			}

			item := util.Item[queueElement, uint64]{
				Value:    queueElement{tuple: tuple, index: i},
				Priority: tuple.Message.LogTime,
			}
			heap.Push(n.pq, &item)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to initialize merge node: %w", err)
	}
	n.initialized = true
	return nil
}

// Next returns the next tuple from the node.
func (n *MergeNode) Next(ctx context.Context) (*Tuple, error) {
	if !n.initialized {
		if err := n.initialize(ctx); err != nil {
			return nil, fmt.Errorf("failed to initialize merge node: %w", err)
		}
	}
	if n.pq.Len() > 0 {
		element := heap.Pop(n.pq).(queueElement)
		next, err := n.children[element.index].Next(ctx)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("failed to get next message on child %d: %w", element.index, err)
		}
		if next != nil {
			item := util.Item[queueElement, uint64]{
				Value:    queueElement{tuple: next, index: element.index},
				Priority: next.Message.LogTime,
			}
			heap.Push(n.pq, &item)
		}
		return element.tuple, nil
	}
	return nil, io.EOF
}

// Close the node.
func (n *MergeNode) Close() error {
	errs := make([]error, 0, len(n.children))
	for _, child := range n.children {
		if err := child.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to close %d children: %v", len(errs), errs)
	}
	return nil
}

// String returns a string representation of the node.
func (n *MergeNode) String() string {
	sb := strings.Builder{}
	sb.WriteString("[merge")
	for _, child := range n.children {
		sb.WriteString(" ")
		sb.WriteString(child.String())
	}
	sb.WriteString("]")
	return sb.String()
}
