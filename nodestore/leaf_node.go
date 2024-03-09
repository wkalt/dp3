package nodestore

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	fmcap "github.com/foxglove/mcap/go/mcap"
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

func (n *LeafNode) Size() uint64 {
	return uint64(len(n.data) + 1)
}

func (n *LeafNode) FromBytes(data []byte) error {
	version := data[0]
	if version < 128 {
		return errors.New("not a leaf node")
	}
	n.version = version - 128
	n.data = data[1:]
	return nil
}

func (n *LeafNode) Merge(data []byte) (*LeafNode, error) {
	buf := &bytes.Buffer{}
	err := mcap.Merge(buf, bytes.NewReader(n.data), bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to merge leaf node: %w", err)
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

func NewLeafNode(data []byte) *LeafNode {
	if data == nil {
		data = []byte{}
	}
	return &LeafNode{data: data, version: leafNodeVersion}
}
