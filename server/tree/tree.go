package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/nodestore"
)

/*
The tree package concerns maintenance of individual copy-on-write trees. Trees
are identified with a node ID and have no additional state. Operations on trees
return new root IDs.

The storage of dp3 consists of many trees, which are coordinated by the treemgr
module.

Note on integer overflows:
It is not difficult to construct a tree that spans into the future range of time
where uint64 nanoseconds will overflow, in 2554. While the inner nodes of that
tree, which are denominated in seconds, are fine, any math operations on those
boundaries or the computed boundaries of their children that involve multiplying
by 1e9 will cause a problem.

The guards against overflow are not as strong as they could be, but the way
things are currently written is that in the cases where we compute child
boundaries using nanosecond conversion, we always check first if there is a
child in the corresponding slot to begin, and skip over the child if so. By that
we are assured that any data that would cause an overflow was already written
into the tree somehow. Since the tree input is itself denominated in nanoseconds
and then cast down to seconds, there is no way to "write an overflowed timestamp
to the tree" -- whatever overflowed uint64 we get is just a uint64 from our
perspective.

I believe this all means that as long as you avoid computing start/end bounds of
children that do not exist, your code is safe from overflows.
*/

////////////////////////////////////////////////////////////////////////////////

// NewInsert inserts the provided "data", which is expected to be
// MCAP-serialized content, into an in-memory tree.
//   - root: a template inner node from which to clone the root of the new tree.
//     Used only for dimensions (does not clone children)
//   - version: the version of the data being inserted. This is a monotonically
//     increasing counter derived from the version store.
//   - timestamp: the timestamp of the data being inserted, which can be
//     computed from known leaf widths and tree heights. The submitted data must
//     span at most one leaf node, so the node can be identified with one
//     timestamp, and any timestamp falling on the target leaf is sufficient.
//   - messageKeys: the message keys for the data being inserted, which include
//     timestamps and sequence numbers.
//   - statistics: the statistics for the data being inserted
//   - data: the MCAP-serialized data to be inserted.
func NewInsert(
	ctx context.Context,
	root *nodestore.InnerNode,
	version uint64,
	timestamp uint64,
	messageKeys []nodestore.MessageKey,
	statistics map[string]*nodestore.Statistics,
	data []byte,
) (*MemTree, error) {
	if root == nil {
		return nil, errors.New("root is nil")
	}
	tw := NewMemTree(nodestore.RandomNodeID(), nil)
	if timestamp < root.Start*1e9 || timestamp >= root.End*1e9 {
		return nil, OutOfBoundsError{timestamp, root.Start, root.End}
	}
	clonedRoot := nodestore.NewInnerNode(
		root.Height,
		root.Start,
		root.End,
		len(root.Children),
	)
	ids := make([]nodestore.NodeID, 0, root.Height+1)
	for range root.Height + 1 {
		id := nodestore.RandomNodeID()
		ids = append(ids, id)
	}
	rootID := ids[0]
	if err := tw.Put(ctx, rootID, clonedRoot); err != nil {
		return nil, fmt.Errorf("failed to store new root: %w", err)
	}
	current := clonedRoot
	for i := 1; current.Height > 1; i++ {
		bucket := bucket(timestamp, current)
		bwidth := bwidth(current)
		node := nodestore.NewInnerNode(
			current.Height-1,
			current.Start+bucket*bwidth,
			current.Start+(bucket+1)*bwidth,
			len(current.Children),
		)
		if err := tw.Put(ctx, ids[i], node); err != nil {
			return nil, fmt.Errorf("failed to store inner node: %w", err)
		}
		current.PlaceChild(bucket, ids[i], version, statistics)
		current = node
	}
	nodeID := ids[len(ids)-1]
	bucket := bucket(timestamp, current)

	node := nodestore.NewLeafNode(messageKeys, data, nil, nil)
	if err := tw.Put(ctx, nodeID, node); err != nil {
		return nil, fmt.Errorf("failed to store leaf node: %w", err)
	}
	current.PlaceChild(bucket, nodeID, version, statistics)
	tw.SetRoot(rootID)
	return tw, nil
}

// NewDelete constructs a partial tree that represents the deletion
// of messages in the given range. Start (inclusive) and end (exclusive) times
// are expected in nanoseconds. The tree writer returned by this function is
// expected to be serialized to the WAL and later merged into the main tree by
// the tree manager.
//
// Tombstones are placed in the tree at the location of children that are fully
// contained within the range. The partial tree, consisting of all nodes on the
// path to deletion (excluding the deleted nodes themselves, which have been
// tombstoned in their parents' children arrays), is returned.
//
// When a deletion range intersects a leaf node but does not fully cover it, the
// node is partially deleted. Since the deletion method does no IO with the
// existing tree, it has no way of knowing in this case whether there is data
// already existing in that location. We record an empty leaf node with a
// "delete range" set to the part of the node that should be deleted. During the
// merge step, there are then two cases to consider: either there was previously
// some data in that location, or there was not.
//
// If the location previously had leaf data, we add to the new leaf's Ancestor
// field the node ID of the previous leaf in the same location. On read, the
// tree iterator is responsible for navigating these linked lists and
// merging/skipping data as appropriate.
//
// If the location previously had no leaf data, the overlay is dropped in the
// merge step resulting in no new leaves to the tree.
//
// The root argument is merely used as a template for determining the structure
// of the partial tree -- no storage IO is performed in this function.
func NewDelete(
	ctx context.Context, oldroot *nodestore.InnerNode, version uint64, start uint64, end uint64,
) (*MemTree, error) {
	if start >= end {
		return nil, fmt.Errorf("invalid range: start %d cannot be >= end %d", start, end)
	}
	if start >= oldroot.End*1e9 || end < oldroot.Start*1e9 {
		return nil, fmt.Errorf("range [%d, %d) out of bounds [%d, %d)", start, end, oldroot.Start*1e9, oldroot.End*1e9)
	}
	root := nodestore.NewInnerNode(oldroot.Height, oldroot.Start, oldroot.End, len(oldroot.Children))
	tw := NewMemTree(nodestore.RandomNodeID(), root)
	stack := []*nodestore.InnerNode{root}
	leafData := &bytes.Buffer{}
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for i := range node.Children {
			childStart := node.Start + bwidth(node)*uint64(i)
			childEnd := node.Start + bwidth(node)*uint64(i+1)
			// The child is entirely outside the range.
			if start >= childEnd*1e9 || end <= childStart*1e9 {
				continue
			}
			// The entire child is within the range.
			if start <= childStart*1e9 && end >= childEnd*1e9 {
				node.PlaceTombstoneChild(uint64(i), version)
				continue
			}
			// Start, end, or both intersect the child. Treat both same as start.
			if node.Height > 1 {
				newChild := nodestore.NewInnerNode(node.Height-1, childStart, childEnd, len(node.Children))
				newChildID := nodestore.RandomNodeID()
				if err := tw.Put(ctx, newChildID, newChild); err != nil {
					return nil, fmt.Errorf("failed to store new inner node: %w", err)
				}
				if childStart*1e9 <= start && start < childEnd*1e9 {
					node.PlaceChild(bucket(start, node), newChildID, version, nil)
				} else {
					node.PlaceChild(bucket(end, node), newChildID, version, nil)
				}
				stack = append(stack, newChild)
				continue
			}
			// One level above the leaf node, deleting partial leaf.
			deleteRangeStart := max(start, childStart*1e9)
			deleteRangeEnd := min(end, childEnd*1e9)
			if err := writeEmptyMCAP(leafData); err != nil {
				return nil, fmt.Errorf("failed to write empty MCAP: %w", err)
			}
			leafID := nodestore.RandomNodeID()
			leaf := nodestore.NewLeafNode(nil, leafData.Bytes(), nil, nil)
			leaf.DeleteRange(deleteRangeStart, deleteRangeEnd)
			if err := tw.Put(ctx, leafID, leaf); err != nil {
				return nil, fmt.Errorf("failed to store new leaf node: %w", err)
			}
			node.PlaceChild(bucket(deleteRangeStart, node), leafID, version, nil)
			leafData.Reset()
		}
	}
	return tw, nil
}

// GetStatRange returns the statistics for the given range of time, for the tree
// rooted at rootID. The granularity parameter is interpreted as a "maximum
// granularity". The returned granularity is guaranteed to be at least as fine
// as the one requested, and in practice can be finer. This can lead to
// confusing results so clients must be prepared to handle it.
func GetStatRange(
	ctx context.Context,
	tr TreeReader,
	start uint64,
	end uint64,
	granularity uint64,
) ([]nodestore.StatRange, error) {
	ranges := []nodestore.StatRange{}
	stack := []nodestore.NodeID{tr.Root()}
	for len(stack) > 0 {
		nodeID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node, err := tr.Get(ctx, nodeID)
		if err != nil {
			return nil, fmt.Errorf("failed to get node %s: %w", nodeID, err)
		}
		switch node := node.(type) {
		case *nodestore.InnerNode:
			width := bwidth(node)
			granularEnough := 1e9*width <= granularity
			for i, child := range node.Children {
				childStart := 1e9 * (node.Start + width*uint64(i))
				childEnd := 1e9 * (node.Start + width*uint64(i+1))
				inRange := child != nil && start <= childEnd && end > childStart
				if inRange && granularEnough {
					for schemaHash, statistics := range child.Statistics {
						statranges, err := statistics.Ranges(childStart, childEnd, schemaHash)
						if err != nil {
							return nil, fmt.Errorf("failed to get statistics ranges: %w", err)
						}
						ranges = append(ranges, statranges...)
					}
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

func writeEmptyMCAP(buf *bytes.Buffer) error {
	writer, err := mcap.NewWriter(buf)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	if err := writer.WriteHeader(&fmcap.Header{}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}
