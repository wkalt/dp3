package nodestore

import (
	"encoding/json"
	"fmt"
)

type LeafNode struct {
	Data []byte `json:"data"`
}

func (n *LeafNode) ToBytes() ([]byte, error) {
	bytes, err := json.Marshal(n)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize data node: %w", err)
	}
	return bytes, nil
}

func (n *LeafNode) FromBytes(data []byte) error {
	if err := json.Unmarshal(data, n); err != nil {
		return fmt.Errorf("error unmarshalling data node: %w", err)
	}
	return nil
}

func (n *LeafNode) Type() NodeType {
	return Leaf
}

func (n *LeafNode) Merge(other *LeafNode) (*LeafNode, error) {
	return NewLeafNode(append(n.Data, other.Data...)), nil
}

func NewLeafNode(data []byte) *LeafNode {
	return &LeafNode{Data: data}
}
