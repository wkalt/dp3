package nodestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wkalt/dp3/server/util"
)

/*
Inner nodes are the interior nodes of the tree. They contain lists of children
containing child node IDs and statistics. Inner nodes are expected to be in
cache, so traversing them will generally be fast and not require disk.

Currently we just serialize the inner nodes as JSON. Once we have field-level
statistics on the messages, the dimensions of the inner node will significantly
change, and we will be in a better position to design a compact format.
*/

////////////////////////////////////////////////////////////////////////////////

// innerNodeVersion is the physical version of the node. Each node's physical
// serialization starts with an unsigned byte indicating version, and also
// allowing us to distinguish between leaf and inner nodes. Inner nodes get
// bytes 0-128 and leaf nodes get 129-256.
const innerNodeVersion = uint8(1)

// InnerNode represents an interior node in the tree, with slots for branchingFactor
// children.
type InnerNode struct {
	Start    uint64   `json:"start"`
	End      uint64   `json:"end"`
	Height   uint8    `json:"height"` // distance from leaf
	Children []*Child `json:"children"`

	version uint8
}

// Child represents a child of an inner node.
type Child struct {
	ID         NodeID                 `json:"id"`
	Version    uint64                 `json:"version"`
	Statistics map[string]*Statistics `json:"statistics"`
}

var ErrNoStatsFound = errors.New("no statistics found")

func (c *Child) GetNumStat(field string) (*NumericalSummary, error) {
	s, err := NewNumericalSummary()
	if err != nil {
		return nil, err
	}
	var found bool
	for _, stats := range c.Statistics {
		for i, f := range stats.Fields {
			if f.Name == field {
				if numStats, ok := stats.NumStats[i]; ok {
					if err := s.Merge(numStats); err != nil {
						return nil, err
					}
					found = true
				}
			}
		}
	}
	if !found {
		return nil, ErrNoStatsFound
	}
	return s, nil
}

type MessageSummary struct {
	Count             int64     `json:"count"`
	BytesUncompressed int64     `json:"bytesUncompressed"`
	SchemaHashes      []string  `json:"schemaHashes"`
	MinObservedTime   time.Time `json:"minObservedTime"`
	MaxObservedTime   time.Time `json:"maxObservedTime"`
}

func (c *Child) MessageSummary() MessageSummary {
	s := MessageSummary{}
	minObserved := make([]int64, 0, len(c.Statistics))
	maxObserved := make([]int64, 0, len(c.Statistics))
	for hash, stats := range c.Statistics {
		s.Count += stats.MessageCount
		s.BytesUncompressed += stats.BytesUncompressed
		s.SchemaHashes = append(s.SchemaHashes, hash)
		minObserved = append(minObserved, stats.MinObservedTime)
		maxObserved = append(maxObserved, stats.MaxObservedTime)
	}
	s.MinObservedTime = time.Unix(0, util.Reduce(util.Min, minObserved[0], minObserved[1:]))
	s.MaxObservedTime = time.Unix(0, util.Reduce(util.Max, maxObserved[0], maxObserved[1:]))
	return s
}

func (c *Child) GetTextStat(field string) (*TextSummary, error) {
	s := &TextSummary{}
	var found bool
	for _, stats := range c.Statistics {
		for i, f := range stats.Fields {
			if f.Name == field {
				if textStats, ok := stats.TextStats[i]; ok {
					s.Merge(textStats)
					found = true
				}
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("field %s not found", field)
	}
	return s, nil
}

// IsTombstone returns true if the child is a tombstone.
func (c *Child) IsTombstone() bool {
	return c.ID == NodeID{} && c.Version > 0
}

func (c *Child) Clone() *Child {
	clone := *c
	clone.Statistics = make(map[string]*Statistics, len(c.Statistics))
	for k, v := range c.Statistics {
		clone.Statistics[k] = v.Clone()
	}
	return &clone
}

// Size returns the size of the node in bytes.
func (n *InnerNode) Size() uint64 {
	return 8 + 8 + 1 + uint64(len(n.Children)*24)
}

// ToBytes serializes the node to a byte array.
func (n *InnerNode) ToBytes() []byte {
	bytes, err := json.Marshal(n)
	if err != nil { // remove when we compact the format and support NaN
		panic("JSON serialization error: " + err.Error())
	}
	buf := make([]byte, len(bytes)+1)
	buf[0] = n.version
	copy(buf[1:], bytes)
	return buf
}

// FromBytes deserializes the node from a byte array.
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

func (n *InnerNode) Clone() *InnerNode {
	clone := *n
	clone.Children = make([]*Child, len(n.Children))
	for i, child := range n.Children {
		if child != nil {
			clone.Children[i] = child.Clone()
		}
	}
	return &clone
}

// PlaceChild sets the child at the given index to the given ID and version.
func (n *InnerNode) PlaceChild(index uint64, id NodeID, version uint64, statistics map[string]*Statistics) {
	n.Children[index] = &Child{
		ID:         id,
		Version:    version,
		Statistics: statistics,
	}
}

// PlaceTombstoneChild inserts a tombstone for the child at the given index with the given version.
func (n *InnerNode) PlaceTombstoneChild(index uint64, version uint64) {
	n.Children[index] = &Child{
		ID:         NodeID{},
		Version:    version,
		Statistics: nil,
	}
}

// Type returns the type of the node.
func (n *InnerNode) Type() NodeType {
	return Inner
}

// NewInnerNode creates a new inner node with the given height and range.
func NewInnerNode(height uint8, start, end uint64, branchingFactor int) *InnerNode {
	return &InnerNode{
		Start:    start,
		End:      end,
		Height:   height,
		Children: make([]*Child, branchingFactor),
		version:  innerNodeVersion,
	}
}
