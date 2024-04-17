package rootmap

import (
	"context"
	"sort"

	"github.com/wkalt/dp3/nodestore"
)

/*
memrootmap is an in-memory implementation of the rootmap interface. It is only
suitable for usage in testing.
*/

////////////////////////////////////////////////////////////////////////////////

type root struct {
	database   string
	producerID string
	topic      string
	version    uint64
	prefix     string
	timestamp  string
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

func (rm *memrootmap) GetHistorical(
	ctx context.Context,
	database string,
	producer string,
	topic string,
) ([]RootListing, error) {
	result := []RootListing{}
	for _, root := range rm.roots {
		if root.database != database {
			continue
		}
		if root.producerID != producer {
			continue
		}
		if root.topic != topic {
			continue
		}
		result = append(result, RootListing{
			root.prefix, root.producerID, root.topic, root.nodeID, root.version, root.timestamp, 0,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Version > result[j].Version
	})
	return result, nil
}

func (rm *memrootmap) GetLatestByTopic(
	ctx context.Context,
	database string,
	producerID string,
	topics map[string]uint64,
) ([]RootListing, error) {
	groups := make(map[string][]root)
	for i := len(rm.roots) - 1; i >= 0; i-- {
		root := rm.roots[i]
		if root.database != database {
			continue
		}
		if root.producerID != producerID {
			continue
		}
		if _, ok := topics[root.topic]; ok {
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
		listings = append(listings, RootListing{
			max.prefix, producerID, topic, max.nodeID, max.version, max.timestamp, topics[topic],
		})
	}
	return listings, nil
}

func (rm *memrootmap) GetLatest(
	ctx context.Context,
	database string,
	producerID string,
	topic string,
) (string, nodestore.NodeID, uint64, error) {
	for i := len(rm.roots) - 1; i >= 0; i-- { // nb: assumes roots added in ascending order
		root := rm.roots[i]
		if root.database == database && root.producerID == producerID && root.topic == topic {
			return root.prefix, root.nodeID, root.version, nil
		}
	}
	return "", nodestore.NodeID{}, 0, NewStreamNotFoundError(database, producerID, topic)
}

func (rm *memrootmap) Get(
	ctx context.Context,
	database string,
	producerID string,
	topic string,
	version uint64,
) (string, nodestore.NodeID, error) {
	for i := len(rm.roots) - 1; i >= 0; i-- {
		root := rm.roots[i]
		if root.database == database && root.producerID == producerID && root.topic == topic && root.version == version {
			return root.prefix, root.nodeID, nil
		}
	}
	return "", nodestore.NodeID{}, NewStreamNotFoundError(database, producerID, topic)
}

func (rm *memrootmap) Put(
	ctx context.Context,
	database string,
	producerID string,
	topic string,
	version uint64,
	prefix string,
	nodeID nodestore.NodeID,
) error {
	timestamp := ""
	rm.roots = append(rm.roots, root{
		database:   database,
		producerID: producerID,
		topic:      topic,
		version:    version,
		timestamp:  timestamp,
		prefix:     prefix,
		nodeID:     nodeID,
	})
	return nil
}
