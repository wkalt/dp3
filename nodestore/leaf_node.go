package nodestore

import "encoding/json"

// todo: compact format
// * leaf data should be a chunk-compressed mcap file
// * summary data should be the mcap summary section
// * summary data should be written ahead of the chunked content on
//   serialization, to allow a reader to read just that portion, without a need
//   for extra storage of an offset or additional seek.
// * version should be excluded from the node, and associated with the edge on
//   the inner node instead.
// * readers will leverage the leaf node as another level in the heirarchy,
//   allowing them to pinpoint byte ranges down to ~8MB decompressed, out of the
//   50-100MB target leaf size. This will give us a reasonable degree of
//   granularity in reads, without requiring tiny files.

// leafNode represents a leaf node in the tree
type LeafNode struct {
	Version uint64   `json:"version"`
	Records []Record `json:"records"`
}

// record represents a timestamped byte array
type Record struct {
	Time uint64 `json:"time"`
	Data []byte `json:"data"`
}

// NewRecord creates a new record with the given time and data.
func NewRecord(time uint64, data []byte) Record {
	return Record{
		Time: time,
		Data: data,
	}
}

// toBytes serializes the node to a byte array.
func (n *LeafNode) ToBytes() []byte {
	bytes, _ := json.Marshal(n)
	return bytes
}

// fromBytes deserializes the node from a byte slice.
func (n *LeafNode) FromBytes(data []byte) error {
	return json.Unmarshal(data, n)
}

func (n *LeafNode) Type() NodeType {
	return Leaf
}

func NewLeafNode(version uint64, records []Record) *LeafNode {
	return &LeafNode{
		Version: version,
		Records: records,
	}
}
