package nodestore

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/wkalt/dp3/server/util"
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

	ancestor            NodeID
	ancestorVersion     uint64
	ancestorDeleteStart uint64
	ancestorDeleteEnd   uint64

	messageKeys []MessageKey
	data        []byte
}

const leafHeaderLength = 1 + 24 + 8 + 8 + 8

func (n *LeafNode) EncodeTo(w io.Writer) error {
	keydata := serializeKeys(n.messageKeys)
	buf := make([]byte, leafHeaderLength+len(n.data)+8+len(keydata))
	offset := util.U8(buf, n.leafNodeVersion+128)
	offset += copy(buf[offset:], n.ancestor[:])
	offset += util.U64(buf[offset:], n.ancestorVersion)
	offset += util.U64(buf[offset:], n.ancestorDeleteStart)
	offset += util.U64(buf[offset:], n.ancestorDeleteEnd)
	offset += util.U64(buf[offset:], uint64(len(keydata)))
	offset += copy(buf[offset:], keydata)
	_, err := w.Write(buf[:offset])
	if err != nil {
		return fmt.Errorf("failed to write leaf node header: %w", err)
	}
	_, err = io.Copy(w, n.Data())
	if err != nil {
		return fmt.Errorf("failed to write leaf node data: %w", err)
	}
	return nil
}

// ToBytes serializes the node to a byte slice.
func (n *LeafNode) ToBytes() []byte {
	buf := make([]byte, leafHeaderLength+8+12*len(n.messageKeys)+len(n.data))
	offset := util.U8(buf, n.leafNodeVersion+128)
	offset += copy(buf[offset:], n.ancestor[:])
	offset += util.U64(buf[offset:], n.ancestorVersion)
	offset += util.U64(buf[offset:], n.ancestorDeleteStart)
	offset += util.U64(buf[offset:], n.ancestorDeleteEnd)

	keydata := serializeKeys(n.messageKeys)
	offset += util.U64(buf[offset:], uint64(len(keydata)))
	offset += copy(buf[offset:], keydata)
	copy(buf[offset:], n.data)
	return buf
}

// Size returns the size of the node in bytes.
func (n *LeafNode) Size() uint64 {
	return uint64(len(n.data) + 1)
}

// AncestorDeleted returns true if the ancestor node has been partially or fully
// deleted.
func (n *LeafNode) AncestorDeleted() bool {
	return n.ancestorDeleteEnd > 0
}

// AncestorDeleteStart returns the start of the ancestor deletion range.
func (n *LeafNode) AncestorDeleteStart() uint64 {
	return n.ancestorDeleteStart
}

// MessageKeys returns the message keys of the node.
func (n *LeafNode) MessageKeys() []MessageKey {
	return n.messageKeys
}

// AncestorDeleteEnd returns the end of the ancestor deletion range.
func (n *LeafNode) AncestorDeleteEnd() uint64 {
	return n.ancestorDeleteEnd
}

// ReadLeafHeader reads the header of a leaf node from a reader, populating the
// node with the data read and returning the length of the header.
func ReadLeafHeader(r io.Reader, node *LeafNode) (int, error) {
	header := make([]byte, leafHeaderLength)
	_, err := io.ReadFull(r, header)
	if err != nil {
		return 0, fmt.Errorf("failed to read leaf node header: %w", err)
	}
	var offset int
	var version uint8
	offset += util.ReadU8(header, &version)
	if version < 128 {
		return 0, errors.New("not a leaf node")
	}
	node.leafNodeVersion = version - 128
	offset += copy(node.ancestor[:], header[offset:])
	offset += util.ReadU64(header[offset:], &node.ancestorVersion)
	offset += util.ReadU64(header[offset:], &node.ancestorDeleteStart)
	offset += util.ReadU64(header[offset:], &node.ancestorDeleteEnd)

	keydataLen, err := util.DecodeU64(r)
	if err != nil {
		return 0, fmt.Errorf("failed to read key data length: %w", err)
	}
	offset += 8
	footer := make([]byte, keydataLen)
	_, err = io.ReadFull(r, footer)
	if err != nil {
		return 0, fmt.Errorf("failed to read key data: %w", err)
	}
	offset += int(keydataLen)
	keys, err := deserializeKeys(footer[:keydataLen])
	if err != nil {
		return 0, fmt.Errorf("failed to deserialize message keys: %w", err)
	}
	node.messageKeys = keys
	return offset, nil
}

// FromBytes deserializes the node from a byte slice.
func (n *LeafNode) FromBytes(data []byte) error {
	hlen, err := ReadLeafHeader(bytes.NewReader(data), n)
	if err != nil {
		return fmt.Errorf("failed to read leaf node header: %w", err)
	}
	n.data = data[hlen:]
	return nil
}

// Data returns the data of the node.
func (n *LeafNode) Data() io.ReadSeeker {
	return bytes.NewReader(n.data)
}

// Type returns the type of the node.
func (n *LeafNode) Type() NodeType {
	return Leaf
}

// Ancestor returns the ID of the ancestor node.
func (n *LeafNode) Ancestor() NodeID {
	return n.ancestor
}

// SetMessageKeys sets the message keys of the node.
func (n *LeafNode) SetMessageKeys(keys []MessageKey) {
	n.messageKeys = keys
}

// HasAncestor returns true if the node has an ancestor.
func (n *LeafNode) HasAncestor() bool {
	return n.ancestor != NodeID{}
}

// AncestorVersion returns the version of the ancestor node.
func (n *LeafNode) AncestorVersion() uint64 {
	return n.ancestorVersion
}

// DeleteRange sets the ancestor deletion metadata to cover the supplied range.
func (n *LeafNode) DeleteRange(start, end uint64) {
	n.ancestorDeleteStart = start
	n.ancestorDeleteEnd = end
}

// NewLeafNode creates a new leaf node with the given data.
func NewLeafNode(
	messageKeys []MessageKey,
	data []byte,
	ancestor *NodeID,
	ancestorVersion *uint64,
) *LeafNode {
	if data == nil {
		data = []byte{}
	}
	if messageKeys == nil {
		messageKeys = []MessageKey{}
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
		messageKeys:     messageKeys,
		data:            data,
	}
}
