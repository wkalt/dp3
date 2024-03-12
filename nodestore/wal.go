package nodestore

import "context"

/*
The WAL is used to aggregate data across multiple writes before flushing to
final storage, while still guaranteeeing those writes are durably persisted.

We do not currently merge the WAL with resultsets on read. That seems like a
logical thing to do but may complicate the clustering story.

Staging through the WAL is useful because it decouples the size of our data
writes from the size of our input files. The worst case scenario for us is
repeated writes to the same leaf file, requiring repeated reading, merging, and
rewriting of the same data from object storage. By staging through the WAL we
can deal with these problems when they are cheap to handle - on local disk.
*/

////////////////////////////////////////////////////////////////////////////////

// WALEntry is a single entry in the WAL.
type WALEntry struct {
	RootID     NodeID
	ProducerID string
	Topic      string
	NodeID     NodeID
	Version    uint64
	Data       []byte
	Deleted    bool
}

// WALListing is a listing of the WAL.
type WALListing struct {
	RootID     NodeID
	ProducerID string
	Topic      string
	Versions   map[uint64][]NodeID
}

// WAL is the interface for the Write Ahead Log.
type WAL interface {
	Put(ctx context.Context, entry WALEntry) error
	Get(ctx context.Context, nodeID NodeID) ([]byte, error)
	List(ctx context.Context) ([]WALListing, error)
	Delete(ctx context.Context, nodeID NodeID) error
}
