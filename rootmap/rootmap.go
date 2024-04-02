package rootmap

import (
	"context"

	"github.com/wkalt/dp3/nodestore"
)

/*
The rootmap is an association between (producerID, topic, version) and available
root node IDs in storage. Every write and read operation must ultimately consult
the rootmap, to figure out what data to merge with or read.

This makes the rootmap a critical component of the system. If you lose the
rootmap the storage files are opaque and would require significant analysis to
recover.
*/

////////////////////////////////////////////////////////////////////////////////

type Rootmap interface {
	GetLatest(ctx context.Context, producerID string, topic string) (nodestore.NodeID, uint64, error)
	GetLatestByTopic(ctx context.Context, producerID string, topics []string) ([]nodestore.NodeID, uint64, error)
	Get(ctx context.Context, producerID string, topic string, version uint64) (nodestore.NodeID, error)
	Put(ctx context.Context, producerID string, topic string, version uint64, nodeID nodestore.NodeID) error
}
