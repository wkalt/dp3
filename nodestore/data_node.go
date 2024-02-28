package nodestore

import (
	"encoding/json"
	"fmt"
)

type DataNode struct {
	Data []byte `json:"data"`
}

func (n *DataNode) ToBytes() ([]byte, error) {
	bytes, err := json.Marshal(n)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize data node: %w", err)
	}
	return bytes, nil
}

func (n *DataNode) FromBytes(data []byte) error {
	if err := json.Unmarshal(data, n); err != nil {
		return fmt.Errorf("error unmarshalling data node: %w", err)
	}
	return nil
}

func (n *DataNode) Type() NodeType {
	return Data
}

func (n *DataNode) Merge(other *DataNode) (*DataNode, error) {
	return NewDataNode(append(n.Data, other.Data...)), nil
}

func NewDataNode(data []byte) *DataNode {
	return &DataNode{Data: data}
}
