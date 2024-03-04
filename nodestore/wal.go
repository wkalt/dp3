package nodestore

type WALEntry struct {
	ID    string
	Paths []NodeID
}

type WAL interface {
	Put(string, []NodeID) error
	Get(string) ([]NodeID, error)
	List() ([]WALEntry, error)
}
