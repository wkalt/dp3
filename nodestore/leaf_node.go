package nodestore

import (
	"bytes"
	"fmt"
	"io"

	"github.com/wkalt/dp3/mcap"
)

const leafNodeVersion = uint8(1)

type LeafNode struct {
	version uint8
	data    []byte
}

func (n *LeafNode) ToBytes() []byte {
	buf := make([]byte, len(n.data)+1)
	buf[0] = n.version + 128
	copy(buf[1:], n.data)
	return buf
}

func (n *LeafNode) FromBytes(data []byte) error {
	version := data[0]
	if version < 128 {
		return fmt.Errorf("not a leaf node")
	}
	n.version = version - 128
	n.data = data[1:]
	return nil
}

func (n *LeafNode) Merge(data []byte) (*LeafNode, error) {
	buf := &bytes.Buffer{}
	err := mcap.Merge(buf, bytes.NewReader(n.data), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return NewLeafNode(buf.Bytes()), nil
}

func (n *LeafNode) Data() io.ReadSeeker {
	return bytes.NewReader(n.data)
}

func (n *LeafNode) Type() NodeType {
	return Leaf
}

func (n *LeafNode) String() string {
	return fmt.Sprintf("[leaf %d]", len(n.data))
}

func NewLeafNode(data []byte) *LeafNode {
	if data == nil {
		data = []byte{}
	}
	return &LeafNode{data: data, version: leafNodeVersion}
}
