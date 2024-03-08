package rootmap

import (
	"context"

	"github.com/wkalt/dp3/nodestore"
)

type root struct {
	producerID string
	topic      string
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

func (rm *memrootmap) GetLatest(
	ctx context.Context, producerID string, topic string) (nodestore.NodeID, uint64, error) {
	for i := len(rm.roots) - 1; i >= 0; i-- { // nb: assumes roots added in ascending order
		root := rm.roots[i]
		if root.producerID == producerID && root.topic == topic {
			return root.nodeID, root.version, nil
		}
	}
	return nodestore.NodeID{}, 0, StreamNotFoundError{producerID, topic}
}

func (rm *memrootmap) Get(
	ctx context.Context, producerID string, topic string, version uint64) (nodestore.NodeID, error) {
	for i := len(rm.roots) - 1; i >= 0; i-- {
		root := rm.roots[i]
		if root.producerID == producerID && root.topic == topic && root.version == version {
			return root.nodeID, nil
		}
	}
	return nodestore.NodeID{}, StreamNotFoundError{producerID, topic}
}

func (rm *memrootmap) Put(
	ctx context.Context, producerID string, topic string,
	version uint64, nodeID nodestore.NodeID) error {
	rm.roots = append(rm.roots, root{
		producerID: producerID,
		topic:      topic,
		version:    version,
		nodeID:     nodeID,
	})
	return nil
}
