package nodestore

type NodeType int

const (
	Inner NodeType = iota + 1
	Leaf
)

func (n NodeType) String() string {
	switch n {
	case Inner:
		return "inner"
	case Leaf:
		return "leaf"
	default:
		return "unknown"
	}
}

// Node is an interface to which leaf and inner nodes adhere.
type Node interface {
	// ToBytes serializes the node to a byte slice
	ToBytes() []byte

	// FromBytes deserializes the node from a byte slice
	FromBytes(data []byte) error

	// Type returns the type of the node
	Type() NodeType
}
