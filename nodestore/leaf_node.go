package nodestore

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
)

/*
Leaf nodes are the lowest level of the tree. They contain the actual data. They
are serialized as MCAP files.
*/

////////////////////////////////////////////////////////////////////////////////

// leafNodeVersion is the current physical version of the leaf nodes. When we
// serialize the node we translate this by 128 to allow us to distinguish inner
// nodes from leaf nodes in deserialization.
const leafNodeVersion = uint8(1)

type LeafNode struct {
	version uint8
	data    []byte
}

// ToBytes serializes the node to a byte slice.
func (n *LeafNode) ToBytes() []byte {
	buf := make([]byte, len(n.data)+1)
	buf[0] = n.version + 128
	copy(buf[1:], n.data)
	return buf
}

// Size returns the size of the node in bytes.
func (n *LeafNode) Size() uint64 {
	return uint64(len(n.data) + 1)
}

// FromBytes deserializes the node from a byte slice.
func (n *LeafNode) FromBytes(data []byte) error {
	version := data[0]
	if version < 128 {
		return errors.New("not a leaf node")
	}
	n.version = version - 128
	n.data = data[1:]
	return nil
}

// Merge merges the given data with the node's data and returns a new leaf node.
func (n *LeafNode) Merge(data []byte) (*LeafNode, error) {
	buf := &bytes.Buffer{}
	err := mcap.Merge(buf, bytes.NewReader(n.data), bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to merge leaf node: %w", err)
	}
	return NewLeafNode(buf.Bytes()), nil
}

// Data returns the data of the node.
func (n *LeafNode) Data() io.ReadSeeker {
	return bytes.NewReader(n.data)
}

// Type returns the type of the node.
func (n *LeafNode) Type() NodeType {
	return Leaf
}

// String returns a string representation of the node.
func (n *LeafNode) String() string {
	reader, err := fmcap.NewReader(bytes.NewReader(n.data))
	if err != nil {
		return fmt.Sprintf("[leaf <unknown %d bytes>]", len(n.data))
	}
	info, err := reader.Info()
	if err != nil {
		return fmt.Sprintf("[leaf <unknown %d bytes>]", len(n.data))
	}
	if info.Statistics.MessageCount == 1 {
		return "[leaf 1 msg]"
	}
	return fmt.Sprintf("[leaf %d msgs]", info.Statistics.MessageCount)
}

// NewLeafNode creates a new leaf node with the given data.
func NewLeafNode(data []byte) *LeafNode {
	if data == nil {
		data = []byte{}
	}
	return &LeafNode{data: data, version: leafNodeVersion}
}
