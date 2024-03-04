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
	"sync"

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
	data, err := n.store.GetRange(ctx, id.OID(), id.Offset(), id.Length())
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, ErrNodeNotFound
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

func (n *Nodestore) ListWAL() ([]WALEntry, error) {
	return n.wal.List()
}

func (n *Nodestore) WALFlush(walid string, ids []NodeID) error {
	if err := n.wal.Put(walid, ids); err != nil {
		return fmt.Errorf("failed to write WAL: %w", err)
	}
	return nil
}

// Stage a node in the staging map, returning an ID that can be used later.
func (n *Nodestore) Stage(node Node) (NodeID, error) {
	id := n.generateStagingID()
	n.mtx.Lock()
	defer n.mtx.Unlock()
	n.staging[id] = node
	return id, nil
}

func (n *Nodestore) GetStagedNode(id NodeID) (Node, bool) {
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
// condition.
func (n *Nodestore) Flush(ctx context.Context, ids ...NodeID) ([]NodeID, error) {
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
		node, ok := n.GetStagedNode(id)
		if !ok {
			return nil, ErrNodeNotStaged
		}
		if inner, ok := node.(*InnerNode); ok {
			for _, child := range inner.Children {
				if child != nil {
					if remapped, ok := processed[child.ID]; ok {
						child.ID = remapped
					}
				}
			}
		}
		n, err := buf.Write(node.ToBytes())
		if err != nil {
			return nil, fmt.Errorf("failed to write node to buffer: %w", err)
		}
		nodeID := generateNodeID(oid, offset, n)
		offset += n
		processed[id] = nodeID
		newIDs[i] = nodeID
	}
	if err := n.store.Put(ctx, oid.String(), buf.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to put object: %w", err)
	}
	slices.Reverse(newIDs)
	return newIDs, nil
}

// NewNodestore creates a new nodestore.
func NewNodestore(
	store storage.Provider,
	cache *util.LRU[NodeID, Node],
) *Nodestore {
	return &Nodestore{
		store:      store,
		cache:      cache,
		nextNodeID: 1,
		maxNodeID:  1e6, // todo: grab this from persistent storage on startup
		mtx:        &sync.RWMutex{},
		staging:    make(map[NodeID]Node),
	}
}

// NewRoot creates a new root node with the given depth and range, and persists
// it to storage, returning the ID.
func (ns *Nodestore) NewRoot(ctx context.Context, start, end uint64, leafWidthSecs int, bfactor int) (nodeID NodeID, err error) {
	minspan := end - start
	depth := uint8(0)
	coverage := uint64(leafWidthSecs)
	for coverage < minspan {
		coverage *= uint64(bfactor)
		depth++
	}
	root := NewInnerNode(depth, start, end, bfactor)
	tmpid, err := ns.Stage(root)
	if err != nil {
		return nodeID, fmt.Errorf("failed to stage root: %w", err)
	}
	ids, err := ns.Flush(ctx, tmpid)
	if err != nil {
		return nodeID, fmt.Errorf("failed to flush root: %w", err)
	}
	return ids[0], nil
}
