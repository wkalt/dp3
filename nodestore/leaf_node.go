package nodestore

type LeafNode struct {
	Data []byte
}

func (n *LeafNode) ToBytes() ([]byte, error) {
	return n.Data, nil
}

func (n *LeafNode) FromBytes(data []byte) error {
	n.Data = data
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
