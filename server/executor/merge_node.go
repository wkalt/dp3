package executor

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/wkalt/dp3/server/util"
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
	pq       *util.PriorityQueue[queueElement]

	initialized bool

	mtx *sync.Mutex
}

// NewMergeNode returns a new merge node.
// The descending parameter specifies the order in which tuples should be
// popped from the priority queue.
func NewMergeNode(descending bool, children ...Node) *MergeNode {
	return &MergeNode{
		children: children,
		pq: util.NewPriorityQueue(func(a, b queueElement) bool {
			if a.tuple.message.LogTime == b.tuple.message.LogTime {
				return a.index < b.index
			}
			if descending {
				return a.tuple.message.LogTime > b.tuple.message.LogTime
			}
			return a.tuple.message.LogTime < b.tuple.message.LogTime
		}),
		mtx:         &sync.Mutex{},
		initialized: false,
	}
}

// initialize pushes one message from each child into the priority queue,
// concurrently.
func (n *MergeNode) initialize(ctx context.Context) error {
	g := errgroup.Group{}
	g.SetLimit(len(n.children))
	for i, child := range n.children {
		child := child
		g.Go(func() error {
			tuple, err := child.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return fmt.Errorf("failed to get next message on child %d: %w", i, err)
			}
			n.mtx.Lock()
			heap.Push(n.pq, queueElement{tuple: tuple, index: i})
			n.mtx.Unlock()
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
		element, ok := heap.Pop(n.pq).(queueElement)
		if !ok {
			return nil, errors.New("failed to pop element from priority queue")
		}
		next, err := n.children[element.index].Next(ctx)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("failed to get next message on child %d: %w", element.index, err)
		}
		if next != nil {
			heap.Push(n.pq, queueElement{tuple: next, index: element.index})
		}
		return element.tuple, nil
	}
	return nil, io.EOF
}

// Close the node.
func (n *MergeNode) Close(ctx context.Context) error {
	if err := util.CloseAllContext(ctx, n.children...); err != nil {
		return fmt.Errorf("failed to close children: %w", err)
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
