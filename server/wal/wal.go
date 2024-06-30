package wal

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/foxglove/mcap/go/mcap"
)

/*
Common types used by the WAL package.
*/

////////////////////////////////////////////////////////////////////////////////

// RecordType is the type of record in the WAL.
type RecordType uint8

func (r RecordType) String() string {
	switch r {
	case WALInsert:
		return "insert"
	case WALMergeRequest:
		return "mergeRequest"
	case WALMergeComplete:
		return "mergeComplete"
	default:
		return "invalid"
	}
}

const (
	WALInvalid RecordType = iota
	WALInsert
	WALMergeRequest
	WALMergeComplete
)

const (
	gigabyte = 1 << 30
)

// InsertRecord is a record of a single insert operation. The data is a byte
// representation of a partial tree.
type InsertRecord struct {
	Database string
	Producer string
	Topic    string
	BatchID  string
	Addr     Address
	Schemas  []*mcap.Schema
	Data     []byte
}

// MergeRequestRecord records a request to merge a batch of inserts into
// storage.
type MergeRequestRecord struct {
	Database string
	Producer string
	Topic    string
	BatchID  string
	Addrs    []Address
}

// MergeCompleteRecord records the completion of a merge into storage.
type MergeCompleteRecord struct {
	BatchID string
}

// Batch represents a collection of inserts that will be merged into the tree
// together. The WAL manager maintains a batch for each producer/topic combo
// that is receiving writes, and intelligently dispatches them for merging based
// on either size or inactivity.
type Batch struct {
	ID         string
	Database   string
	Producer   string
	Topic      string
	Size       int
	LastUpdate time.Time
	Addrs      []Address
	OnDone     func() error
}

func (b Batch) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", b.Database, b.Producer, b.Topic, b.ID)
}

func (b *Batch) Finish() error {
	return b.OnDone()
}

// Addresses are 24-byte values consisting of an object ID, offset, and length.
type Address [24]byte

// NewAddress creates a new address from the given object, offset, and length.
func NewAddress(object, offset, length uint64) Address {
	var a Address
	binary.LittleEndian.PutUint64(a[:8], object)
	binary.LittleEndian.PutUint64(a[8:16], offset)
	binary.LittleEndian.PutUint64(a[16:24], length)
	return a
}

func (a Address) object() string {
	object := binary.LittleEndian.Uint64(a[:8])
	return strconv.FormatUint(object, 10)
}

func (a Address) offset() int64 {
	return int64(binary.LittleEndian.Uint64(a[8:16]))
}

func (a Address) length() int {
	return int(binary.LittleEndian.Uint64(a[16:24]))
}

func (a Address) String() string {
	return fmt.Sprintf("%d:%d:%d",
		binary.LittleEndian.Uint64(a[:8]),
		binary.LittleEndian.Uint64(a[8:16]),
		binary.LittleEndian.Uint64(a[16:24]),
	)
}

// TreeID is a unique identifier for a tree in the WAL.
type TreeID struct {
	Database string
	Producer string
	Topic    string
}

func NewTreeID(database, producer, topic string) TreeID {
	return TreeID{Database: database, Producer: producer, Topic: topic}
}

func (id TreeID) String() string {
	return id.Database + id.Producer + id.Topic
}
