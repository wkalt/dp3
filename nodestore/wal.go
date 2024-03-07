package nodestore

import "context"

type WALEntry struct {
	StreamID string
	NodeID   NodeID
	Version  uint64
	Data     []byte
	Deleted  bool
}

type WALListing struct {
	StreamID string
	Versions map[uint64][]NodeID
}

type WAL interface {
	Put(ctx context.Context, entry WALEntry) error
	GetStream(ctx context.Context, streamID string) ([][]NodeID, error)
	Get(ctx context.Context, nodeID NodeID) ([]byte, error)
	List(ctx context.Context) ([]WALListing, error)
	Delete(ctx context.Context, nodeID NodeID) error
}
