package nodestore

import (
	"encoding/json"
	"errors"
	"fmt"
)

const innerNodeVersion = uint8(1)

// todo: compact format
// * child list may compress well if we number the tree sequentially.
// * otoh we may need to think about the scheme, to allow a sequential numbering
//   of ids that is also shared across trees that don't exist yet.
// * version should be out of the node body and into the children array.

// innerNode represents an interior node in the tree, with slots for 64
// children.
type InnerNode struct {
	Start    uint64   `json:"start"`
	End      uint64   `json:"end"`
	Depth    uint8    `json:"depth"`
	Children []*Child `json:"children"`

	version uint8
}

type Child struct {
	ID      NodeID `json:"id"`
	Version uint64 `json:"version"`
}

// toBytes serializes the node to a byte array.
func (n *InnerNode) ToBytes() []byte {
	bytes, _ := json.Marshal(n) // nolint temporary
	buf := make([]byte, len(bytes)+1)
	buf[0] = n.version
	copy(buf[1:], bytes)
	return buf
}

func (n *InnerNode) FromBytes(data []byte) error {
	version := data[0]
	if version >= 128 {
		return errors.New("not an inner node")
	}
	n.version = version
	err := json.Unmarshal(data[1:], n)
	if err != nil {
		return fmt.Errorf("error unmarshalling inner node: %w", err)
	}
	return nil
}

func (n *InnerNode) PlaceChild(index uint64, id NodeID, version uint64) {
	n.Children[index] = &Child{
		ID:      id,
		Version: version,
	}
}

func (n *InnerNode) Type() NodeType {
	return Inner
}

func NewInnerNode(depth uint8, start, end uint64, branchingFactor int) *InnerNode {
	return &InnerNode{
		Start:    start,
		End:      end,
		Depth:    depth,
		Children: make([]*Child, branchingFactor),
		version:  innerNodeVersion,
	}
}
