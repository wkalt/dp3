package tree

import (
	"context"
	"errors"
	"fmt"

	"github.com/wkalt/dp3/nodestore"
)

/*
The tree package concerns maintenance of individual copy-on-write trees. Trees
are identified with a node ID and have no additional state. Operations on trees
return new root IDs.

The storage of dp3 consists of many trees, which are coordinated by the treemgr
module.
*/

// //////////////////////////////////////////////////////////////////////////////

// StatRange is a range of multigranular statistics computed on inner nodes.
type StatRange struct {
	Start      uint64                `json:"start"`
	End        uint64                `json:"end"`
	Statistics *nodestore.Statistics `json:"statistics"`
}

// GetStatRange returns the statistics for the given range of time, for the tree
// rooted at rootID. The granularity parameter is interpreted as a "maximum
// granularity". The returned granularity is guaranteed to be at least as fine
// as the one requested, and in practice can be considerably finer. This can
// lead to confusing results so clients must be prepared to handle it.
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
			granularEnough := 1e9*bwidth(node) <= granularity
			width := bwidth(node)
			for i, child := range node.Children {
				childStart := 1e9 * (node.Start + width*uint64(i))
				childEnd := 1e9 * (node.Start + width*uint64(i+1))
				inRange := child != nil && start <= childEnd && end > childStart
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

// Insert stages leaf data into the tree rooted at nodeID, in the leaf location
// appropriate for the supplied timestamp.
//
// All new nodes down to the leaf are assigned the version parameter as the
// version, and associated with the provided statistics record.
// new leaf are assigned the version parameter.
//
// If there is no error, a new root node ID is returned, corresponding to the root
// of the new tree. At the time it is returned, the new node exists only in the nodestore's
// staging map. The caller is responsible for flushing the ID to WAL.
//
// On error, the rootID is returned unchanged, and the path is nil.
func Insert(
	ctx context.Context,
	ns *nodestore.Nodestore,
	rootID nodestore.NodeID,
	version uint64,
	timestamp uint64,
	data []byte,
	statistics *nodestore.Statistics,
) (newRootID nodestore.NodeID, path []nodestore.NodeID, err error) {
	root, err := cloneInnerNode(ctx, ns, rootID)
	if err != nil {
		return rootID, nil, err
	}
	if timestamp < root.Start*1e9 || timestamp >= root.End*1e9 {
		return rootID, nil, OutOfBoundsError{timestamp, root.Start, root.End}
	}
	rootID = ns.Stage(root)
	nodes := []nodestore.NodeID{rootID}
	current := root
	for current.Height > 1 {
		current, err = descend(ctx, ns, &nodes, current, timestamp, version, statistics)
		if err != nil {
			return rootID, nil, err
		}
	}
	bucket := bucket(timestamp, current)
	node := nodestore.NewLeafNode(data)
	stagedID := ns.Stage(node) // todo: should insert flush to WAL?
	nodes = append(nodes, stagedID)
	current.PlaceChild(bucket, stagedID, version, statistics)
	return nodes[0], nodes, nil
}

// bwidth returns the width of each bucket in seconds.
func bwidth(n *nodestore.InnerNode) uint64 {
	bwidth := (n.End - n.Start) / uint64(len(n.Children))
	return bwidth
}

// bucket returns the index of the child slot that the given time falls into on
// the given node.
func bucket(nanos uint64, n *nodestore.InnerNode) uint64 {
	bwidth := bwidth(n)
	bucket := (nanos - n.Start*1e9) / (1e9 * bwidth)
	return bucket
}

// cloneInnerNode returns a new inner node with the same contents as the node
// with the given id, but with the version changed to the one supplied.
func cloneInnerNode(ctx context.Context, ns *nodestore.Nodestore, id nodestore.NodeID) (*nodestore.InnerNode, error) {
	node, err := ns.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to clone inner node %d: %w", id, err)
	}
	oldNode, ok := node.(*nodestore.InnerNode)
	if !ok {
		return nil, newUnexpectedNodeError(nodestore.Inner, node)
	}
	newNode := nodestore.NewInnerNode(oldNode.Height, oldNode.Start, oldNode.End, len(oldNode.Children))
	for i := range oldNode.Children {
		if oldNode.Children[i] != nil {
			newNode.Children[i] = &nodestore.Child{
				ID:      oldNode.Children[i].ID,
				Version: oldNode.Children[i].Version,
			}
		}
	}
	return newNode, nil
}

// descend the tree to the node that contains the given timestamp, copying nodes
// at each step and recording the path taken.
func descend(
	ctx context.Context,
	ns *nodestore.Nodestore,
	nodeIDs *[]nodestore.NodeID,
	current *nodestore.InnerNode,
	timestamp uint64,
	version uint64,
	stats *nodestore.Statistics,
) (node *nodestore.InnerNode, err error) {
	bucket := bucket(timestamp, current)
	if existing := current.Children[bucket]; existing != nil {
		node, err = cloneInnerNode(ctx, ns, existing.ID)
		if err != nil {
			return nil, err
		}
	} else {
		bwidth := bwidth(current)
		node = nodestore.NewInnerNode(
			current.Height-1,
			current.Start+bucket*bwidth,
			current.Start+(bucket+1)*bwidth,
			len(current.Children),
		)
	}
	nodeID := ns.Stage(node)
	current.PlaceChild(bucket, nodeID, version, stats)
	*nodeIDs = append(*nodeIDs, nodeID)
	return node, nil
}
