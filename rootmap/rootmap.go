package rootmap

import (
	"context"

	"github.com/wkalt/dp3/nodestore"
)

type Rootmap interface {
	GetLatest(ctx context.Context, streamID string) (nodestore.NodeID, uint64, error)
	Get(ctx context.Context, streamID string, version uint64) (nodestore.NodeID, error)
	Put(ctx context.Context, producerID string, topic string,
		streamID string, version uint64, nodeID nodestore.NodeID) error
}
