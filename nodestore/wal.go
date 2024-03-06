package nodestore

import "context"

type WALEntry struct {
	StreamID string
	NodeID   NodeID
	Version  uint64
	Data     []byte
}

type WALListing struct {
	StreamID string
	Versions map[uint64][]NodeID
}

type WAL interface {
	Put(context.Context, WALEntry) error
	GetStream(context.Context, string) ([][]NodeID, error)
	Get(context.Context, NodeID) ([]byte, error)
	List(context.Context) ([]WALListing, error)
}
