package tree

import (
	"context"
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

////////////////////////////////////////////////////////////////////////////////

// Insert stages leaf data into the tree rooted at nodeID, in the leaf location
// appropriate for the supplied timestamp.
//
// All new nodes down to the leaf are assigned the version parameter as the
// version, and associated with the provided statistics record.
// new leaf are assigned the version parameter.
//
// A new root node ID is returned, corresponding to the root of the new tree. At
// the time it is returned, the new node exists only in the nodestore's staging
// map. The caller is responsible for flushing the ID to WAL.
func Insert(
	ctx context.Context,
	ns *nodestore.Nodestore,
	nodeID nodestore.NodeID,
	version uint64,
	start uint64,
	data []byte,
	statistics *nodestore.Statistics,
) (rootID nodestore.NodeID, path []nodestore.NodeID, err error) {
	root, err := cloneInnerNode(ctx, ns, nodeID)
	if err != nil {
		return rootID, nil, err
	}
	if start < root.Start*1e9 || start >= root.End*1e9 {
		return rootID, nil, OutOfBoundsError{start, root.Start, root.End}
	}
	rootID = ns.Stage(root)
	nodes := []nodestore.NodeID{rootID}
	current := root
	for current.Depth > 1 {
		current, err = descend(ctx, ns, &nodes, current, start, version, statistics)
		if err != nil {
			return rootID, nil, err
		}
	}
	bucket := bucket(start, current)
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
	newNode := nodestore.NewInnerNode(oldNode.Depth, oldNode.Start, oldNode.End, len(oldNode.Children))
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
			current.Depth-1,
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
