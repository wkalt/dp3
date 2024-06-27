package nodestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/wkalt/dp3/server/storage"
	"github.com/wkalt/dp3/server/util"
)

/*
dp3 is based on a copy-on-write time-partitioned tree structure. The tree
contains inner nodes and leaf nodes. Leaf nodes hold the data, and inner nodes
hold aggregate statistics and pointers to children. Trees are fixed-height and a
typical height is 5 (just an example).

The Nodestore is responsible for providing access to nodes by node ID. It does
this using a heirarchical storage scheme:
* Permanent storage is "store", a storage.Provider. In production
  contexts this will be S3-compatible object storage.
* Nodes are cached on read in a byte capacity-limited LRU cache. If future reads
  reference the same node, it will be served from cache. Nodes are never modified.
  In ideal operation, all inner nodes of the tree are cached in "cache".
* An insert to the tree returns a "memtree", which is a linked representation of
  a partial tree overlay. Once any required modifications are performed, this
  structure is serialized to a byte array and written to the WAL, and from there
  to storage.
*/

////////////////////////////////////////////////////////////////////////////////

type Nodestore struct {
	store storage.Provider
	cache *util.LRU[NodeID, Node]

	mtx *sync.RWMutex
}

// NewNodestore creates a new nodestore.
func NewNodestore(
	store storage.Provider,
	cache *util.LRU[NodeID, Node],
) *Nodestore {
	return &Nodestore{
		store: store,
		cache: cache,
		mtx:   &sync.RWMutex{},
	}
}

// Put an object to storage.
func (n *Nodestore) Put(ctx context.Context, prefix string, oid uint64, r io.Reader) error {
	objectname := prefix + "/" + strconv.FormatUint(oid, 10)
	if err := n.store.Put(ctx, objectname, r); err != nil {
		return fmt.Errorf("failed to put object %d: %w", oid, err)
	}
	return nil
}

// Get retrieves a node from the nodestore. It will check the cache prior to
// storage.
func (n *Nodestore) Get(ctx context.Context, prefix string, id NodeID) (Node, error) {
	if value, ok := n.cache.Get(id); ok {
		util.IncContextValue(ctx, "node_cache_hits", 1)
		return value, nil
	}
	util.IncContextValue(ctx, "node_cache_misses", 1)
	reader, err := n.store.GetRange(ctx, prefix+"/"+id.Object(), int(id.Offset()), int(id.Length()))
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, NodeNotFoundError{prefix, id}
		}
		return nil, fmt.Errorf("failed to get node %s: %w", id, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read node: %w", err)
	}
	node, err := BytesToNode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node: %w", err)
	}
	n.cache.Put(id, node, node.Size())
	return node, nil
}

// GetLeafNode retrieves a leaf node from the nodestore. It returns a ReadSeekCloser
// over the leaf data, the closing of which is the caller's responsibility. It
// does not cache data.
func (n *Nodestore) GetLeafNode(ctx context.Context, prefix string, id NodeID) (
	node *LeafNode, reader io.ReadSeekCloser, err error,
) {
	offset := int(id.Offset())
	length := int(id.Length())
	reader, err = n.store.GetRange(ctx, prefix+"/"+id.Object(), offset, length)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, nil, NodeNotFoundError{prefix, id}
		}
		return nil, nil, fmt.Errorf("failed to get node %s: %w", id, err)
	}
	var header LeafNode
	headerlen, err := ReadLeafHeader(reader, &header)
	if err != nil {
		if closeErr := reader.Close(); closeErr != nil {
			return nil, nil, fmt.Errorf("failed to close reader: %w", closeErr)
		}
		return nil, nil, fmt.Errorf("failed to read leaf header: %w", err)
	}
	rsc, err := util.NewReadSeekCloserAt(reader, headerlen, length-headerlen)
	if err != nil {
		if closeErr := reader.Close(); closeErr != nil {
			return nil, nil, fmt.Errorf("failed to close reader: %w", closeErr)
		}
		return nil, nil, fmt.Errorf("failed to create read seek closer: %w", err)
	}
	return &header, rsc, nil
}

func isLeaf(data []byte) bool {
	return data[0] > 128
}

func BytesToNode(value []byte) (Node, error) {
	if isLeaf(value) {
		node := NewLeafNode(nil, nil, nil, nil)
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
