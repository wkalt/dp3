package nodestore

import (
	"context"

	"golang.org/x/exp/maps"
)

/*
memwal is an in-memory implementation of the WAL interface. It is only suitable
for usage in testing.
*/

////////////////////////////////////////////////////////////////////////////////

type memwal struct {
	entries []*WALEntry
}

func NewMemWAL() WAL {
	return &memwal{}
}

func (m *memwal) Put(ctx context.Context, entry WALEntry) error {
	m.entries = append(m.entries, &entry)
	return nil
}

func (m *memwal) Get(ctx context.Context, nodeID NodeID) ([]byte, error) {
	for _, entry := range m.entries {
		if entry.NodeID == nodeID && !entry.Deleted {
			return entry.Data, nil
		}
	}
	return nil, NodeNotFoundError{nodeID}
}

func (m *memwal) Delete(ctx context.Context, nodeID NodeID) error {
	for _, entry := range m.entries {
		if entry.NodeID == nodeID && !entry.Deleted {
			entry.Deleted = true
			return nil
		}
	}
	return NodeNotFoundError{nodeID}
}

func (m *memwal) List(ctx context.Context) ([]WALListing, error) {
	streams := make(map[string]WALListing)
	for _, entry := range m.entries {
		key := entry.ProducerID + entry.Topic
		listing, ok := streams[key]
		if !ok {
			listing = WALListing{
				ProducerID: entry.ProducerID,
				Topic:      entry.Topic,
				Versions:   make(map[uint64][]NodeID),
			}
			streams[key] = listing
		}
		listing.Versions[entry.Version] = append(listing.Versions[entry.Version], entry.NodeID)
		streams[key] = listing
	}
	return maps.Values(streams), nil
}
