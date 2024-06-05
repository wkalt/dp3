package rootmap

import (
	"context"
	"errors"

	"github.com/wkalt/dp3/nodestore"
)

/*
The rootmap is an association between (producer, topic, version) and available
root node IDs in storage. Every write and read operation must ultimately consult
the rootmap, to figure out what data to merge with or read.

This makes the rootmap a critical component of the system. If you lose the
rootmap the storage files are opaque and would require significant analysis to
recover.
*/

////////////////////////////////////////////////////////////////////////////////

var ErrRootAlreadyExists = errors.New("root already exists")

type RootListing struct {
	Prefix     string           `json:"prefix"`
	Producer   string           `json:"producer"`
	Topic      string           `json:"topic"`
	NodeID     nodestore.NodeID `json:"nodeId"`
	Version    uint64           `json:"version"`
	Timestamp  string           `json:"timestamp"`
	MinVersion uint64           `json:"minVersion"`
}

type Rootmap interface {
	NextVersion(
		ctx context.Context,
	) (uint64, error)
	Put(
		ctx context.Context,
		database string,
		producer string,
		topic string,
		version uint64,
		prefix string,
		nodeID nodestore.NodeID,
	) error
	Truncate(
		ctx context.Context,
		database string,
		producer string,
		topic string,
		timestamp int64,
	) error
	GetLatest(
		ctx context.Context,
		database string,
		producer string,
		topic string,
	) (string, nodestore.NodeID, uint64, uint64, error)
	GetLatestByTopic(
		ctx context.Context,
		database string,
		producer string,
		topics map[string]uint64,
	) ([]RootListing, error)
	Get(
		ctx context.Context,
		database string,
		producer string,
		topic string,
		version uint64,
	) (string, nodestore.NodeID, error)
	GetHistorical(
		ctx context.Context,
		database string,
		producer string,
		topic string,
	) ([]RootListing, error)

	Topics(ctx context.Context, database string) ([]string, error)
	Producers(ctx context.Context, database string) ([]string, error)
	Databases(ctx context.Context) ([]string, error)
}

type table struct {
	database string
	producer string
	topic    string
}
