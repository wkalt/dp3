package rootmap

import (
	"github.com/google/uuid"
	"github.com/wkalt/dp3/nodestore"
)

type Rootmap interface {
	GetLatest(streamID uuid.UUID) (nodestore.NodeID, error)
	Get(streamID uuid.UUID, version uint64) (nodestore.NodeID, error)
	Put(streamID uuid.UUID, version uint64, nodeID nodestore.NodeID) error
}
