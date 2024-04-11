package nodestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/wkalt/dp3/storage"
	"github.com/wkalt/dp3/util"
)

/*
dp3 is based on a copy-on-write time-partitioned tree structure. The tree
contains inner nodes and leaf nodes. Leaf nodes hold the data, and inner nodes
hold aggregate statistics and pointers to children. Trees are fixed-height and a
typical height is 5 (just an example).

The Nodestore is responsible for providing access to nodes by node ID. It does
this using a heirarchical storage scheme:
* Nodes are initially written to a WAL. After a period of time, they are flushed
  from WAL to permanent storage. This allows for write batching to counteract
  variations in input file sizes.
* Permanent storage is "store", a storage.Provider. In production
  contexts this will be S3-compatible object storage.
* Nodes are cached on read in a byte capacity-limited LRU cache. If future reads
  reference the same node, it will be served from cache. Nodes are never modified.
  In ideal operation, all inner nodes of the tree are cached in "cache".
* During the course of insert and WAL flush operations, nodes are staged in an
  in-memory map ("staging") to support temporary manipulations prior to final
  persistence.

I think the staging map can and should probably go away, if the tree can
communicate to the nodestore about the association between temporary node IDs
and the in-memory nodes. Currently Insert returns a path of node IDs and the
order is significant, so a bit of refactoring will be required beyond just
changing the return type to a map.

The nodestore is also responsible for orchestrating tree merge operations across
the levels of its hierarchy, such as flushing staged nodes to WAL, or WAL nodes
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
func (n *Nodestore) Put(ctx context.Context, prefix string, oid uint64, data []byte) error {
	objectname := prefix + "/" + strconv.FormatUint(oid, 10)
	if err := n.store.Put(ctx, objectname, data); err != nil {
		return fmt.Errorf("failed to put object %d: %w", oid, err)
	}
	return nil
}

// Get retrieves a node from the nodestore. It will check the cache prior to
// storage.
func (n *Nodestore) Get(ctx context.Context, prefix string, id NodeID) (Node, error) {
	if value, ok := n.cache.Get(id); ok {
		return value, nil
	}
	reader, err := n.store.GetRange(ctx, prefix+"/"+id.OID(), int(id.Offset()), int(id.Length()))
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
	node, err := n.BytesToNode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node: %w", err)
	}
	n.cache.Put(id, node, node.Size())
	return node, nil
}

// GetLeaf retrieves a leaf node from the nodestore. It returns a ReadSeekCloser
// over the leaf data, the closing of which is the caller's responsibility. It
// does not cache data.
func (n *Nodestore) GetLeafData(ctx context.Context, prefix string, id NodeID) (
	ancestor NodeID, reader io.ReadSeekCloser, err error,
) {
	reader, err = n.store.GetRange(ctx, prefix+"/"+id.OID(), int(id.Offset()), int(id.Length()))
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return ancestor, nil, NodeNotFoundError{prefix, id}
		}
		return ancestor, nil, fmt.Errorf("failed to get node %s: %w", id, err)
	}
	buf := make([]byte, 1+8+24)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return ancestor, nil, fmt.Errorf("failed to read leaf node: %w", err)
	}
	ancestor = NodeID(buf[1+8:])
	return ancestor, reader, nil
}

func isLeaf(data []byte) bool {
	return data[0] > 128
}

func (n *Nodestore) BytesToNode(value []byte) (Node, error) {
	if isLeaf(value) {
		node := NewLeafNode(nil, nil, nil)
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
