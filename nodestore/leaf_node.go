package nodestore

import (
	"bytes"
	"io"

	"github.com/wkalt/dp3/mcap"
)

const leafNodeVersion = uint8(1)

type LeafNode struct {
	version uint8
	data    []byte
}

func (n *LeafNode) ToBytes() ([]byte, error) {
	buf := make([]byte, len(n.data)+1)
	buf[0] = n.version + 128
	copy(buf[1:], n.data)
	return buf, nil
}

func (n *LeafNode) FromBytes(data []byte) error {
	n.version = data[0] - 128
	n.data = data[1:]
	return nil
}

func (n *LeafNode) Merge(other *LeafNode) (*LeafNode, error) {
	buf := &bytes.Buffer{}
	err := mcap.Merge(buf, bytes.NewReader(n.data), bytes.NewReader(other.data))
	if err != nil {
		return nil, err
	}
	return NewLeafNode(buf.Bytes()), nil
}

func (n *LeafNode) Len() int {
	return len(n.data)
}

func (n *LeafNode) Data() io.Reader {
	return bytes.NewReader(n.data)
}

func (n *LeafNode) Type() NodeType {
	return Leaf
}

func NewLeafNode(data []byte) *LeafNode {
	if data == nil {
		data = []byte{}
	}
	return &LeafNode{data: data, version: leafNodeVersion}
}
