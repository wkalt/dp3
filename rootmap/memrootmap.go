package rootmap

import (
	"context"

	"github.com/wkalt/dp3/nodestore"
)

/*
memrootmap is an in-memory implementation of the rootmap interface. It is only
suitable for usage in testing.
*/

////////////////////////////////////////////////////////////////////////////////

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

func (rm *memrootmap) GetLatestByTopic(
	ctx context.Context,
	producerID string,
	topics map[string]uint64,
) ([]RootListing, error) {
	groups := make(map[string][]root)
	for i := len(rm.roots) - 1; i >= 0; i-- {
		root := rm.roots[i]
		if _, ok := topics[root.topic]; ok && root.producerID == producerID {
			groups[root.topic] = append(groups[root.topic], root)
		}
	}
	var maxVersion uint64
	listings := make([]RootListing, 0, len(topics))
	for topic, group := range groups {
		max := group[0]
		for _, item := range group {
			if item.version > max.version {
				max = item
			}
			if item.version > maxVersion {
				maxVersion = item.version
			}
		}
		listings = append(listings, RootListing{topic, max.nodeID, max.version, topics[topic]})
	}
	return listings, nil
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
