package rootmap

import (
	"github.com/wkalt/dp3/nodestore"
)

type Rootmap interface {
	GetLatest(streamID string) (nodestore.NodeID, error)
	Get(streamID string, version uint64) (nodestore.NodeID, error)
	Put(streamID string, version uint64, nodeID nodestore.NodeID) error
}
