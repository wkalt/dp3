package nodestore

type WALEntry struct {
	StreamID string
	NodeID   NodeID
	Version  uint64
	Data     []byte
}

type WAL interface {
	Put(WALEntry) error
	GetStream(streamID string) ([][]NodeID, error)
	Get(nodeID NodeID) ([]byte, error)
	List() ([]WALEntry, error)
}
