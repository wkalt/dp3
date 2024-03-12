package nodestore

/*
Nodes in the tree adhere to the Node interface. There are currently two kinds of
node in the tree - inner nodes and leaf nodes.

Inner nodes are nodes on the interior of the tree. They hold references to their
children (by node ID), as well a statistical aggregates about their children.

Leaf nodes are nodes at the leaves of the tree. They hold messages that fall
into whatever time window they describe (which is determined by the parameters
of the tree root).

One weakness of the current design is that for large topics like images, the
leaves can end up large, like 250MB. Pulling 250MB out of storage just to read
some messages will hurt time to first byte for streaming playback. There are at
least three ways we can address this - currently we're waiting for more info to
pick.

1. Currently tree dimensions are hardcoded but we plan to dimension the tree
based on an initial sampling of messages. Once we have this we'll be in position
to really understand the effects of different parameter choices. It is possible
that simply sizing the tree to target 4MB leaves will solve the issue.

2. Currently we read leaf nodes and inner nodes the same way - slurp the whole
thing into cache before using it. Leaves are MCAP files though, and MCAP files
contain indexes that support time-based remote traversal. We can make the leaf
readers smarter to exploit this, without a new node type.

3. We can extend the tree's set of nodes to include a new "super-leaf node"
containing MCAP summary sections, effectively embedding the MCAP file index in
our tree structure. The leaf nodes would then be MCAP chunks rather than files.
I'm not too wild about this idea currently because I think it'll damage our
ability to use off-the-shelf MCAP tooling. However, we are eventually going to
need our own tooling anyway.
*/

////////////////////////////////////////////////////////////////////////////////

// NodeType is an enum for the type of node.
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

	// Size returns the size of the node in bytes, used for LRU cache
	// accounting. This isn't a strictly accurate size in terms of golang memory
	// representation, but it's sufficient.
	Size() uint64
}
