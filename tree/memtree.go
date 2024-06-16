package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
)

/*
MemTree is an in-memory implementation of a tree, with serialization methods
to/from disk. It is used for building trees in memory, either while receiving
input data and preparing to flush to WAL, or when reading from the WAL and
preparing to serialize to storage.
*/

//////////////////////////////////////////////////////////////////////////////

// MemTree implements both TreeReader and TreeWriter.
type MemTree struct {
	root  nodestore.NodeID
	nodes map[nodestore.NodeID]nodestore.Node
}

// Get returns the node with the given ID.
func (m *MemTree) Get(ctx context.Context, id nodestore.NodeID) (nodestore.Node, error) {
	node, ok := m.nodes[id]
	if !ok {
		return nil, nodestore.NodeNotFoundError{NodeID: id}
	}
	return node, nil
}

// GetLeafNode returns the data for a leaf node.
func (m *MemTree) GetLeafNode(
	ctx context.Context, id nodestore.NodeID,
) (*nodestore.LeafNode, io.ReadSeekCloser, error) {
	node, err := m.Get(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	leaf, ok := node.(*nodestore.LeafNode)
	if !ok {
		return nil, nil, errors.New("node is not a leaf")
	}
	return leaf, util.NewReadSeekNopCloser(leaf.Data()), nil
}

// Put inserts a node into the MemTree.
func (m *MemTree) Put(ctx context.Context, id nodestore.NodeID, node nodestore.Node) error {
	if m.nodes == nil {
		m.nodes = make(map[nodestore.NodeID]nodestore.Node)
	}
	m.nodes[id] = node
	return nil
}

// Root returns the root node ID.
func (m *MemTree) Root() nodestore.NodeID {
	return m.root
}

// Problem: how does this know not to pursue links into other structures?

// FromBytes deserializes the byte array into the memtree. Memtrees are
// serialized to disk as bytetrees.
func (m *MemTree) FromBytes(ctx context.Context, data []byte) error {
	bt := byteTree{r: bytes.NewReader(data)}
	rootID := bt.Root()
	object := rootID.Object()
	m.SetRoot(rootID)
	ids := []nodestore.NodeID{rootID}
	for len(ids) > 0 {
		id := ids[0]
		ids = ids[1:]
		node, err := bt.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("failed to look up node %s: %w", id, err)
		}
		if err := m.Put(ctx, id, node); err != nil {
			return fmt.Errorf("failed to put node %s: %w", id, err)
		}
		if inner, ok := node.(*nodestore.InnerNode); ok {
			for _, child := range inner.Children {
				if child == nil {
					continue
				}
				if child.IsTombstone() {
					continue
				}
				if child.ID.Object() != object {
					continue
				}
				ids = append(ids, child.ID)
			}
		}
	}
	return nil
}

// ToBytes serializes the memtree to a byte array suitable for storage in the
// WAL or in an object. The nodes are serialized from leaf to root. The IDs are
// boosted by offset. The last 24 bytes are the root ID.
func (m *MemTree) ToBytes(ctx context.Context, oid uint64) ([]byte, error) { // nolint: funlen
	root, err := m.Get(ctx, m.root)
	if err != nil {
		return nil, fmt.Errorf("failed to read root: %w", err)
	}
	node, ok := root.(*nodestore.InnerNode)
	if !ok {
		return nil, NewUnexpectedNodeError(nodestore.Inner, root)
	}
	path := []nodestore.NodeID{m.root}
	nodes := []*nodestore.InnerNode{node}
	for len(nodes) > 0 {
		node, nodes = nodes[len(nodes)-1], nodes[:len(nodes)-1]
		for _, child := range node.Children {
			if child == nil {
				continue
			}
			childNode, err := m.Get(ctx, child.ID)
			if err != nil {
				if errors.Is(err, nodestore.NodeNotFoundError{}) {
					// assume it will inherit from parent during
					// deserialization.
					continue
				}
				return nil, fmt.Errorf("failed to get child node: %w", err)
			}
			path = append(path, child.ID)
			if inner, ok := childNode.(*nodestore.InnerNode); ok {
				nodes = append(nodes, inner)
			}
		}
	}

	// serialize the path in reverse order
	buf := &bytes.Buffer{}
	offset := 0
	slices.Reverse(path)
	processed := make(map[nodestore.NodeID]nodestore.NodeID)
	for _, id := range path {
		node, err := m.Get(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("failed to get node: %w", err)
		}
		if inner, ok := node.(*nodestore.InnerNode); ok {
			for _, child := range inner.Children {
				if child == nil {
					continue
				}
				if remapped, ok := processed[child.ID]; ok {
					child.ID = remapped
				}
			}
		}
		n, err := buf.Write(node.ToBytes())
		if err != nil {
			return nil, fmt.Errorf("failed to write node: %w", err)
		}
		nodeID := nodestore.NewNodeID(oid, uint64(offset), uint64(n))
		offset += n
		processed[id] = nodeID
	}

	// write the root ID onto the end
	remappedRootID := processed[m.root]
	if _, err = buf.Write(remappedRootID[:]); err != nil {
		return nil, fmt.Errorf("failed to write remapped root: %w", err)
	}
	return buf.Bytes(), nil
}

// SetRoot sets the root node of the MemTree.
func (m *MemTree) SetRoot(id nodestore.NodeID) {
	m.root = id
}

// NewMemTree creates a new MemTree with the given root node.
func NewMemTree(rootID nodestore.NodeID, root *nodestore.InnerNode) *MemTree {
	nodes := make(map[nodestore.NodeID]nodestore.Node)
	nodes[rootID] = root
	return &MemTree{root: rootID, nodes: nodes}
}

func (m *MemTree) String(ctx context.Context) string {
	s, err := Print(ctx, m)
	if err != nil {
		return err.Error()
	}
	return s
}
