package nodestore

import "context"

type WALEntry struct {
	RootID     NodeID
	ProducerID string
	Topic      string
	NodeID     NodeID
	Version    uint64
	Data       []byte
	Deleted    bool
}

type WALListing struct {
	RootID     NodeID
	ProducerID string
	Topic      string
	Versions   map[uint64][]NodeID
}

type WAL interface {
	Put(ctx context.Context, entry WALEntry) error
	Get(ctx context.Context, nodeID NodeID) ([]byte, error)
	List(ctx context.Context) ([]WALListing, error)
	Delete(ctx context.Context, nodeID NodeID) error
}
