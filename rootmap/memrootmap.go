package rootmap

import (
	"context"

	"github.com/wkalt/dp3/nodestore"
)

type memrootmap struct {
	m map[string]map[uint64]nodestore.NodeID
}

func NewMemRootmap() Rootmap {
	return &memrootmap{
		m: make(map[string]map[uint64]nodestore.NodeID),
	}
}

func (rm *memrootmap) GetLatest(ctx context.Context, streamID string) (nodestore.NodeID, error) {
	versions, ok := rm.m[streamID]
	if !ok {
		return nodestore.NodeID{}, ErrNodeNotFound
	}
	var latest uint64
	for v := range versions {
		if v > latest {
			latest = v
		}
	}
	return versions[latest], nil
}

func (rm *memrootmap) Get(ctx context.Context, streamID string, version uint64) (nodestore.NodeID, error) {
	versions, ok := rm.m[streamID]
	if !ok {
		return nodestore.NodeID{}, ErrNodeNotFound
	}
	return versions[version], nil
}

func (rm *memrootmap) Put(ctx context.Context, streamID string, version uint64, nodeID nodestore.NodeID) error {
	versions, ok := rm.m[streamID]
	if !ok {
		versions = make(map[uint64]nodestore.NodeID)
		rm.m[streamID] = versions
	}
	versions[version] = nodeID
	return nil
}
