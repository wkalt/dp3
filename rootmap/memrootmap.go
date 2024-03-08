package rootmap

import (
	"context"

	"github.com/wkalt/dp3/nodestore"
)

type root struct {
	streamID   string
	producerID string
	version    uint64
	nodeID     nodestore.NodeID
}

type memrootmap struct {
	roots []root
}

func NewMemRootmap() Rootmap {
	return &memrootmap{
		roots: []root{},
	}
}

func (rm *memrootmap) GetLatest(ctx context.Context, streamID string) (nodestore.NodeID, uint64, error) {
	for i := len(rm.roots) - 1; i >= 0; i-- { // nb: assumes roots added in ascending order
		root := rm.roots[i]
		if root.streamID == streamID {
			return root.nodeID, root.version, nil
		}
	}
	return nodestore.NodeID{}, 0, StreamNotFoundError{streamID}
}

func (rm *memrootmap) Get(ctx context.Context, streamID string, version uint64) (nodestore.NodeID, error) {
	for i := len(rm.roots) - 1; i >= 0; i-- {
		root := rm.roots[i]
		if root.streamID == streamID && root.version == version {
			return root.nodeID, nil
		}
	}
	return nodestore.NodeID{}, StreamNotFoundError{streamID}
}

func (rm *memrootmap) Put(
	ctx context.Context, producerID string, topic string,
	streamID string, version uint64, nodeID nodestore.NodeID) error {
	rm.roots = append(rm.roots, root{
		streamID:   streamID,
		producerID: producerID,
		version:    version,
		nodeID:     nodeID,
	})
	return nil
}
