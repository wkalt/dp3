package nodestore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

func (n *Nodestore) String() string {
	return n.cache.String()
}

type Nodestore struct {
	store      storage.Provider
	cache      *util.LRU[NodeID, Node]
	nextNodeID uint64
	maxNodeID  uint64

	staging map[NodeID]Node
	mtx     *sync.RWMutex

	wal WAL
}

func (n *Nodestore) WALDelete(ctx context.Context, id NodeID) error {
	if err := n.wal.Delete(ctx, id); err != nil {
		return fmt.Errorf("failed to delete node: %w", err)
	}
	return nil
}

// generateStagingID generates a temporary ID that will not collide with "real"
// node IDs.
func (n *Nodestore) generateStagingID() NodeID {
	var id NodeID
	_, _ = rand.Read(id[:])
	return id
}

type objectID uint64

func (id objectID) String() string {
	return strconv.FormatUint(uint64(id), 10)
}

func (n *Nodestore) generateObjectID() objectID {
	id := n.nextNodeID
	n.nextNodeID++
	return objectID(id)
}

func isLeaf(data []byte) bool {
	return data[0] > 128
}

func (n *Nodestore) bytesToNode(value []byte) (Node, error) {
	if isLeaf(value) {
		node := NewLeafNode(nil)
		if err := node.FromBytes(value); err != nil {
			return nil, fmt.Errorf("failed to parse leaf node: %w", err)
		}
		return node, nil
	}
	node := NewInnerNode(0, 0, 0, 0)
	if err := node.FromBytes(value); err != nil {
		return nil, fmt.Errorf("failed to parse inner node: %w", err)
	}
	return node, nil
}

// Get retrieves a node from the store. If the node is not in the cache, it will be loaded from the store.
func (n *Nodestore) Get(ctx context.Context, id NodeID) (Node, error) {
	if value, ok := n.cache.Get(id); ok {
		return value, nil
	}
	if value, ok := n.staging[id]; ok {
		return value, nil
	}
	data, err := n.store.GetRange(ctx, id.OID(), id.Offset(), id.Length())
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, NodeNotFoundError{id}
		}
		return nil, fmt.Errorf("failed to get node: %w", err)
	}
	node, err := n.bytesToNode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node: %w", err)
	}
	n.cache.Put(id, node)
	return node, nil
}

func (n *Nodestore) ListWAL(ctx context.Context) ([]WALListing, error) {
	wal, err := n.wal.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list wal: %w", err)
	}
	return wal, nil
}

func (n *Nodestore) WALFlush(
	ctx context.Context,
	producerID string,
	topic string,
	version uint64,
	ids []NodeID,
) error {
	for _, id := range ids {
		node, err := n.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("failed to get node %s: %w", id, err)
		}
		bytes := node.ToBytes()
		entry := WALEntry{
			ProducerID: producerID,
			Topic:      topic,
			NodeID:     id,
			Version:    version,
			Data:       bytes,
		}
		if err := n.wal.Put(ctx, entry); err != nil {
			return fmt.Errorf("failed to write WAL: %w", err)
		} // todo: make transactional
		delete(n.staging, id)
	}
	return nil
}

// Stage a node in the staging map, returning an ID that can be used later.
func (n *Nodestore) Stage(node Node) NodeID {
	id := n.generateStagingID()
	n.mtx.Lock()
	defer n.mtx.Unlock()
	n.staging[id] = node
	return id
}

func (n *Nodestore) StageWithID(id NodeID, node Node) error {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	n.staging[id] = node
	return nil
}

func (n *Nodestore) getStagedNode(id NodeID) (Node, bool) {
	n.mtx.RLock()
	defer n.mtx.RUnlock()
	node, ok := n.staging[id]
	return node, ok
}

func generateNodeID(oid objectID, offset int, length int) NodeID {
	var id NodeID
	binary.LittleEndian.PutUint64(id[:], uint64(oid))
	binary.LittleEndian.PutUint32(id[8:], uint32(offset))
	binary.LittleEndian.PutUint32(id[12:], uint32(length))
	return id
}

// Flush flushes a list of node IDs to the store in a single object. The IDs are
// assumed to be in root -> leaf order, such that the reversed list will capture
// dependency ordering. All nodes will be removed from staging under any exit
// condition. Existing content in the same logical location is cloned and copied
// into the final output tree.
func (n *Nodestore) Flush(ctx context.Context, version uint64, ids ...NodeID) (newRootID NodeID, err error) {
	defer func() {
		n.mtx.Lock()
		for _, id := range ids {
			delete(n.staging, id)
		}
		n.mtx.Unlock()
	}()
	newIDs := make([]NodeID, len(ids))
	buf := &bytes.Buffer{}
	offset := 0
	slices.Reverse(ids)
	processed := make(map[NodeID]NodeID)
	oid := n.generateObjectID()

	for i, id := range ids {
		node, ok := n.getStagedNode(id)
		if !ok {
			return newRootID, ErrNodeNotStaged
		}

		if inner, ok := node.(*InnerNode); ok {
			for _, child := range inner.Children {
				if child != nil {
					if remapped, ok := processed[child.ID]; ok {
						child.ID = remapped
						child.Version = version
					}
				}
			}
			node = inner
		}
		n, err := buf.Write(node.ToBytes())
		if err != nil {
			return newRootID, fmt.Errorf("failed to write node to buffer: %w", err)
		}
		nodeID := generateNodeID(oid, offset, n)
		offset += n
		processed[id] = nodeID
		newIDs[i] = nodeID
	}
	if err := n.store.Put(ctx, oid.String(), buf.Bytes()); err != nil {
		return newRootID, fmt.Errorf("failed to put object: %w", err)
	}
	return newIDs[len(newIDs)-1], nil
}

// NewNodestore creates a new nodestore.
func NewNodestore(
	store storage.Provider,
	cache *util.LRU[NodeID, Node],
	wal WAL,
) *Nodestore {
	return &Nodestore{
		store:      store,
		cache:      cache,
		nextNodeID: 1,
		maxNodeID:  1e6, // todo: grab this from persistent storage on startup
		mtx:        &sync.RWMutex{},
		staging:    make(map[NodeID]Node),
		wal:        wal,
	}
}

// NewRoot creates a new root node with the given depth and range, and persists
// it to storage, returning the ID.
func (n *Nodestore) NewRoot(
	ctx context.Context,
	start uint64,
	end uint64,
	leafWidthSecs int,
	bfactor int,
) (nodeID NodeID, err error) {
	var depth int
	span := end - start
	coverage := leafWidthSecs
	for uint64(coverage) < span {
		coverage *= bfactor
		depth++
	}
	root := NewInnerNode(uint8(depth), start, start+uint64(coverage), bfactor)
	tmpid := n.Stage(root)
	id, err := n.Flush(ctx, 0, tmpid)
	if err != nil {
		return nodeID, fmt.Errorf("failed to flush root %s: %w", tmpid, err)
	}
	return id, nil
}

func (n *Nodestore) FlushWALPath(ctx context.Context, version uint64, path []NodeID) (nodeID NodeID, err error) {
	result := []NodeID{}
	for _, nodeID := range path {
		data, err := n.wal.Get(ctx, nodeID)
		if err != nil {
			return nodeID, fmt.Errorf("failed to get node %s from wal: %w", nodeID, err)
		}
		node, err := n.bytesToNode(data)
		if err != nil {
			return nodeID, fmt.Errorf("failed to parse node: %w", err)
		}
		if err := n.StageWithID(nodeID, node); err != nil {
			return nodeID, fmt.Errorf("failed to stage node: %w", err)
		}
		result = append(result, nodeID)
	}
	flushedRoot, err := n.Flush(ctx, version, result...)
	if err != nil {
		return nodeID, fmt.Errorf("failed to flush root: %w", err)
	}
	for _, node := range path { // todo transaction
		if err := n.wal.Delete(ctx, node); err != nil {
			return nodeID, fmt.Errorf("failed to delete node: %w", err)
		}
	}
	return flushedRoot, nil
}

// WALMerge merges a list of root node IDs that exist in WAL into a single
// partial tree in the staging area, and then merges that tree into persistent
// storage.
func (n *Nodestore) WALMerge(
	ctx context.Context,
	rootID NodeID,
	version uint64,
	nodeIDs []NodeID,
) (nodeID NodeID, err error) {
	ids := make([]NodeID, len(nodeIDs)+1)
	ids[0] = rootID
	copy(ids[1:], nodeIDs)
	mergedPath, err := n.nodeMerge(ctx, version, ids)
	if err != nil {
		return nodeID, fmt.Errorf("failed to merge nodes: %w", err)
	}
	flushedRoot, err := n.Flush(ctx, version, mergedPath...)
	if err != nil {
		return nodeID, fmt.Errorf("failed to flush root: %w", err)
	}
	return flushedRoot, nil
}

// nodeMerge does an N-way tree merge, returning a "path" from the root to leaf
// of the new tree. All nodes are from the same level of the tree, and can thus
// be assumed to have the same type and same number of children.
func (n *Nodestore) nodeMerge(ctx context.Context, version uint64, nodeIDs []NodeID) ([]NodeID, error) {
	if len(nodeIDs) == 0 {
		return nil, errors.New("no nodes to merge")
	}
	nodes := make([]Node, len(nodeIDs))
	for i, id := range nodeIDs {
		var node Node
		node, err := n.getWALOrStorage(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("failed to parse node: %w", err)
		}
		nodes[i] = node
	}
	switch node := (nodes[0]).(type) {
	case *InnerNode:
		inner := make([]*InnerNode, len(nodes))
		for i, node := range nodes {
			inner[i] = node.(*InnerNode)
		}
		nodeIDs, err := n.mergeInnerNodes(ctx, version, inner)
		if err != nil {
			return nil, err
		}
		return nodeIDs, nil
	case *LeafNode:
		merged, err := n.mergeLeaves(ctx, nodeIDs)
		if err != nil {
			return nil, err
		}
		return []NodeID{merged}, nil
	default:
		return nil, fmt.Errorf("unrecognized node type: %T", node)
	}
}

func (n *Nodestore) mergeInnerNodes(ctx context.Context, version uint64, nodes []*InnerNode) ([]NodeID, error) {
	conflicts := []int{}
	node := nodes[0]
	for i, outer := range node.Children {
		var conflicted bool
		for _, node := range nodes {
			inner := node.Children[i]
			if outer == nil && inner != nil || outer != nil && inner == nil ||
				outer != nil && inner != nil && outer.ID != inner.ID {
				conflicted = true
			}
			if conflicted && !slices.Contains(conflicts, i) {
				conflicts = append(conflicts, i)
			}
		}
	}
	newInner := NewInnerNode(node.Depth, node.Start, node.End, len(node.Children))
	newID := n.Stage(newInner)
	result := []NodeID{newID}
	for _, conflict := range conflicts {
		children := []NodeID{} // set of not-null children mapping to conflicts
		for _, node := range nodes {
			if inner := node.Children[conflict]; inner != nil && !slices.Contains(children, inner.ID) {
				children = append(children, inner.ID)
			}
		}
		merged, err := n.nodeMerge(ctx, version, children) // merged child for this conflict
		if err != nil {
			return nil, err
		}
		newInner.Children[conflict] = &Child{ID: merged[0], Version: version}
		result = append(result, merged...)
	}
	for i := range node.Children {
		if !slices.Contains(conflicts, i) {
			newInner.Children[i] = node.Children[i]
		}
	}
	return result, nil
}

func (n *Nodestore) getWALOrStorage(ctx context.Context, nodeID NodeID) (Node, error) {
	data, err := n.wal.Get(ctx, nodeID)
	if err != nil {
		if errors.Is(err, NodeNotFoundError{}) {
			node, err := n.Get(ctx, nodeID)
			if err != nil {
				return nil, fmt.Errorf("failed to get node %d: %w", nodeID, err)
			}
			return node, nil
		}
		return nil, fmt.Errorf("failed to get node %d from wal: %w", nodeID, err)
	}
	node, err := n.bytesToNode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node: %w", err)
	}
	return node, nil
}

func (n *Nodestore) mergeLeaves(ctx context.Context, nodeIDs []NodeID) (NodeID, error) {
	if len(nodeIDs) == 1 {
		id := nodeIDs[0]
		var node Node
		node, err := n.getWALOrStorage(ctx, id)
		if err != nil {
			return NodeID{}, fmt.Errorf("failed to get node: %w", err)
		}
		nodeID := n.Stage(node)
		return nodeID, nil
	}
	iterators := make([]fmcap.MessageIterator, len(nodeIDs))
	for i, id := range nodeIDs {
		leaf, err := n.getWALOrStorage(ctx, id)
		if err != nil {
			return NodeID{}, fmt.Errorf("failed to get leaf %d from wal: %w", id, err)
		}
		reader, err := fmcap.NewReader(leaf.(*LeafNode).Data())
		if err != nil {
			return NodeID{}, fmt.Errorf("failed to create reader: %w", err)
		}
		defer reader.Close()
		iterators[i], err = reader.Messages()
		if err != nil {
			return NodeID{}, fmt.Errorf("failed to create iterator: %w", err)
		}
	}
	buf := &bytes.Buffer{}
	if err := mcap.Nmerge(buf, iterators...); err != nil {
		return NodeID{}, fmt.Errorf("failed to merge leaves: %w", err)
	}
	newLeaf := NewLeafNode(buf.Bytes())
	return n.Stage(newLeaf), nil
}

func (n *Nodestore) Print(ctx context.Context, nodeID NodeID, version uint64) (string, error) {
	node, err := n.Get(ctx, nodeID)
	if err != nil {
		switch {
		case errors.As(err, &NodeNotFoundError{}):
			if value, err := n.wal.Get(ctx, nodeID); err == nil {
				node, err = n.bytesToNode(value)
				if err != nil {
					return "", fmt.Errorf("failed to parse node: %w", err)
				}
			}
		default:
			return "", fmt.Errorf("failed to get node %d: %w", nodeID, err)
		}
	}
	switch node := node.(type) {
	case *InnerNode:
		return n.printInnerNode(ctx, node, version)
	case *LeafNode:
		return node.String(), nil
	default:
		return "", fmt.Errorf("unexpected node type: %+v", node)
	}
}

func (n *Nodestore) printInnerNode(
	ctx context.Context,
	node *InnerNode,
	version uint64,
) (string, error) {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("[%d-%d:%d", node.Start, node.End, version))
	span := node.End - node.Start
	for i, child := range node.Children {
		if child == nil {
			continue
		}
		childStr, err := n.Print(ctx, child.ID, child.Version)
		if err != nil {
			return "", err
		}
		childNode, err := n.Get(ctx, child.ID)
		if err != nil {
			switch {
			case errors.As(err, &NodeNotFoundError{}):
				if value, err := n.wal.Get(ctx, child.ID); err == nil {
					childNode, err = n.bytesToNode(value)
					if err != nil {
						return "", fmt.Errorf("failed to parse node: %w", err)
					}
				}
			default:
				return "", fmt.Errorf("failed to get node %d: %w", child.ID, err)
			}
		}
		if cnode, ok := childNode.(*LeafNode); ok {
			start := node.Start + uint64(i)*span/uint64(len(node.Children))
			end := node.Start + uint64(i+1)*span/uint64(len(node.Children))
			sb.WriteString(fmt.Sprintf(" [%d-%d:%d %s]", start, end, child.Version, cnode))
		} else {
			sb.WriteString(" " + childStr)
		}
	}
	sb.WriteString("]")
	return sb.String(), nil
}
