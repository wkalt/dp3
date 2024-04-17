package rootmap

import (
	"context"
	"errors"

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

var ErrRootAlreadyExists = errors.New("root already exists")

type RootListing struct {
	Prefix              string           `json:"prefix"`
	Producer            string           `json:"producer"`
	Topic               string           `json:"topic"`
	NodeID              nodestore.NodeID `json:"nodeId"`
	Version             uint64           `json:"version"`
	Timestamp           string           `json:"timestamp"`
	RequestedMinVersion uint64           `json:"requestedMinVersion"`
}

type Rootmap interface {
	GetLatest(
		ctx context.Context,
		database string,
		producerID string,
		topic string,
	) (string, nodestore.NodeID, uint64, error)
	GetLatestByTopic(
		ctx context.Context,
		database string,
		producerID string,
		topics map[string]uint64,
	) ([]RootListing, error)
	Get(
		ctx context.Context,
		database string,
		producerID string,
		topic string,
		version uint64,
	) (string, nodestore.NodeID, error)
	Put(
		ctx context.Context,
		database string,
		producerID string,
		topic string,
		version uint64,
		prefix string,
		nodeID nodestore.NodeID,
	) error
	GetHistorical(
		ctx context.Context,
		database string,
		producer string,
		topic string,
	) ([]RootListing, error)
}
