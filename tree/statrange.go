package tree

import (
	"context"
	"errors"
	"fmt"

	"github.com/wkalt/dp3/nodestore"
)

type StatRange struct {
	Start      uint64                `json:"start"`
	End        uint64                `json:"end"`
	Statistics *nodestore.Statistics `json:"statistics"`
}

func GetStatRange(
	ctx context.Context,
	ns *nodestore.Nodestore,
	rootID nodestore.NodeID,
	start uint64,
	end uint64,
	granularity uint64,
) ([]StatRange, error) {
	ranges := []StatRange{}
	stack := []nodestore.NodeID{rootID}
	for len(stack) > 0 {
		nodeID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node, err := ns.Get(ctx, nodeID)
		if err != nil {
			return nil, fmt.Errorf("failed to get node %s: %w", nodeID, err)
		}
		switch node := node.(type) {
		case *nodestore.InnerNode:
			granularEnough := 1e9*bwidth(node) < granularity
			width := bwidth(node)
			for i, child := range node.Children {
				childStart := 1e9 * (node.Start + width*uint64(i))
				childEnd := 1e9 * (node.Start + width*uint64(i+1))
				inRange := child != nil && start < childEnd && end >= childStart
				if inRange && granularEnough {
					ranges = append(ranges, StatRange{
						Start:      childStart,
						End:        childEnd,
						Statistics: child.Statistics,
					})
					continue
				}
				if inRange {
					stack = append(stack, child.ID)
				}
			}
		case *nodestore.LeafNode:
			// todo: compute exact statistics from the messages
			return nil, errors.New("sorry, too granular")
		}
	}
	return ranges, nil
}
