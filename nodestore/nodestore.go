package nodestore

import (
	"bytes"
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

type Nodestore struct {
	store      storage.Provider
	cache      *util.LRU[NodeID, Node]
	nextNodeID uint64
	maxNodeID  uint64

	staging map[NodeID]Node
	mtx     *sync.RWMutex
}

type NodeID [16]byte

func (n NodeID) OID() string {
	return strconv.FormatUint(binary.LittleEndian.Uint64(n[:8]), 10)
}

func (n NodeID) Offset() int {
	return int(binary.LittleEndian.Uint32(n[8:]))
}

func (n NodeID) Length() int {
	return int(binary.LittleEndian.Uint32(n[12:]))
}

func (n NodeID) String() string {
	return fmt.Sprintf("%s:%d:%d", n.OID(), n.Offset(), n.Length())
}

func (n NodeID) Empty() bool {
	return n == NodeID{}
}

// generateStagingID generates a temporary ID that will not collide with "real"
// node IDs.
func (n *Nodestore) generateStagingID() NodeID {
	var id NodeID
	rand.Read(id[:])
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

func bytesToNode(value []byte) (Node, error) {
	if isLeaf(value) {
		node := NewLeafNode(nil)
		if err := node.FromBytes(value); err != nil {
			return nil, fmt.Errorf("failed to parse leaf node: %w", err)
		}
		return node, nil
	}
	node := NewInnerNode(0, 0, 0)
	if err := node.FromBytes(value); err != nil {
		return nil, fmt.Errorf("failed to parse inner node: %w", err)
	}
	return node, nil
}

// Get retrieves a node from the store. If the node is not in the cache, it will be loaded from the store.
func (n *Nodestore) Get(id NodeID) (Node, error) {
	if value, ok := n.cache.Get(id); ok {
		return value, nil
	}
	data, err := n.store.GetRange(id.OID(), id.Offset(), id.Length())
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, ErrNodeNotFound
		}
		return nil, fmt.Errorf("failed to get node: %w", err)
	}
	node, err := bytesToNode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node: %w", err)
	}
	n.cache.Put(id, node)
	return node, nil
}

// Stage a node in the staging map, returning an ID that can be used later.
func (n *Nodestore) Stage(node Node) (NodeID, error) {
	id := n.generateStagingID()
	n.mtx.Lock()
	defer n.mtx.Unlock()
	n.staging[id] = node
	return id, nil
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
// condition.
func (n *Nodestore) Flush(ids ...NodeID) ([]NodeID, error) {
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
		bytes, err := node.ToBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize node: %w", err)
		}
		n, err := buf.Write(bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to write node to buffer: %w", err)
		}
		nodeID := generateNodeID(oid, offset, n)
		offset += n
		processed[id] = nodeID
		newIDs[i] = nodeID
	}
	err := n.store.Put(oid.String(), buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to put object: %w", err)
	}
	slices.Reverse(newIDs)
	return newIDs, nil
}

// NewNodestore creates a new nodestore.
func NewNodestore(store storage.Provider, cache *util.LRU[NodeID, Node]) *Nodestore {
	return &Nodestore{
		store:      store,
		cache:      cache,
		nextNodeID: 1,
		maxNodeID:  1e6, // todo: grab this from persistent storage on startup
		mtx:        &sync.RWMutex{},
		staging:    make(map[NodeID]Node),
	}
}
