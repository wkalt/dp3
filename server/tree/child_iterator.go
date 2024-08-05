package tree

import (
	"context"
	"errors"
	"fmt"

	"github.com/wkalt/dp3/server/nodestore"
)

// IterateChildren iterates over the children of the tree within the given time
// range at a bucket width at least as small as the one provided. Depending on
// the physical tree dimensions, this can result in a much more granular
// division than the one requested (never less granular). The caller should be
// aware of this and consider pre-aggregating results to the requested
// granularity.
func IterateChildren(
	ctx context.Context,
	tr Reader,
	start uint64,
	end uint64,
	bucketWidthSecs int,
	f func(*nodestore.Child, uint64, uint64) error,
) error {
	stack := []nodestore.NodeID{tr.Root()}
	for len(stack) > 0 {
		nodeID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node, err := tr.Get(ctx, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get node: %w", err)
		}
		switch node := node.(type) {
		case *nodestore.InnerNode:
			width := bwidth(node)
			granularEnough := width <= uint64(bucketWidthSecs)
			for i, child := range node.Children {
				childStart := node.Start + uint64(i)*width
				childEnd := childStart + width
				inRange := child != nil && start <= childEnd && end > childStart
				if inRange && granularEnough {
					if err := f(child, childStart*1e9, childEnd*1e9); err != nil {
						return fmt.Errorf("failed to process child: %w", err)
					}
					continue
				}
				if inRange {
					stack = append(stack, child.ID)
				}
			}
		case *nodestore.LeafNode:
			return errors.New("sorry, too granular")
		}
	}
	return nil
}
