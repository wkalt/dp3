package nodestore

import (
	"encoding/json"
	"fmt"
)

// todo: compact format
// * child list may compress well if we number the tree sequentially.
// * otoh we may need to think about the scheme, to allow a sequential numbering
//   of ids that is also shared across trees that don't exist yet.
// * version should be out of the node body and into the children array.

// innerNode represents an interior node in the tree, with slots for 64
// children.
type InnerNode struct {
	Version  uint64   `json:"version"`
	Start    uint64   `json:"start"`
	End      uint64   `json:"end"`
	Children []uint64 `json:"children"`
}

// toBytes serializes the node to a byte array.
func (n *InnerNode) ToBytes() ([]byte, error) {
	bytes, err := json.Marshal(n)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize inner node: %w", err)
	}
	return bytes, nil
}

func (n *InnerNode) FromBytes(data []byte) error {
	err := json.Unmarshal(data, n)
	if err != nil {
		return fmt.Errorf("error unmarshalling inner node: %w", err)
	}
	return nil
}

func (n *InnerNode) Type() NodeType {
	return Inner
}

func NewInnerNode(start, end uint64, version uint64, branchingFactor int) *InnerNode {
	return &InnerNode{
		Start:    start,
		End:      end,
		Version:  version,
		Children: make([]uint64, branchingFactor),
	}
}
