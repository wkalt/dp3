package nodestore

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/util"
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
	leafNodeVersion uint8
	ancestorVersion uint64
	ancestor        NodeID
	data            []byte
}

// ToBytes serializes the node to a byte slice.
func (n *LeafNode) ToBytes() []byte {
	buf := make([]byte, len(n.data)+1+8+24)
	offset := util.U8(buf, n.leafNodeVersion+128)
	offset += util.U64(buf[offset:], n.ancestorVersion)
	offset += copy(buf[offset:], n.ancestor[:])
	copy(buf[offset:], n.data)
	return buf
}

// Size returns the size of the node in bytes.
func (n *LeafNode) Size() uint64 {
	return uint64(len(n.data) + 1)
}

// FromBytes deserializes the node from a byte slice.
func (n *LeafNode) FromBytes(data []byte) error {
	var version uint8
	var ancestorVersion uint64
	offset := util.ReadU8(data, &version)
	offset += util.ReadU64(data[offset:], &ancestorVersion)
	ancestor := NodeID(data[offset : offset+24])

	if version < 128 {
		return errors.New("not a leaf node")
	}
	n.leafNodeVersion = version - 128
	n.ancestor = ancestor
	n.data = data[offset+24:]
	return nil
}

// Merge merges the given data with the node's data and returns a new leaf node.
func (n *LeafNode) Merge(data []byte) (*LeafNode, error) {
	buf := &bytes.Buffer{}
	err := mcap.Merge(buf, bytes.NewReader(n.data), bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to merge leaf node: %w", err)
	}
	return NewLeafNode(buf.Bytes(), &n.ancestor, &n.ancestorVersion), nil
}

// Data returns the data of the node.
func (n *LeafNode) Data() io.ReadSeeker {
	return bytes.NewReader(n.data)
}

// Type returns the type of the node.
func (n *LeafNode) Type() NodeType {
	return Leaf
}

func (n *LeafNode) Ancestor() NodeID {
	return n.ancestor
}

func (n *LeafNode) AncestorVersion() uint64 {
	return n.ancestorVersion
}

// NewLeafNode creates a new leaf node with the given data.
func NewLeafNode(data []byte, ancestor *NodeID, ancestorVersion *uint64) *LeafNode {
	if data == nil {
		data = []byte{}
	}
	ancID := NodeID{}
	if ancestor != nil {
		ancID = *ancestor
	}

	var ancVersion uint64
	if ancestorVersion != nil {
		ancVersion = *ancestorVersion
	}
	return &LeafNode{
		leafNodeVersion: leafNodeVersion,
		ancestor:        ancID,
		ancestorVersion: ancVersion,
		data:            data,
	}
}
