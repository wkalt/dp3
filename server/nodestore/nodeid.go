package nodestore

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

/*
Node IDs in dp3 are 24 bytes long. The first 8 bytes is an unsigned 64-bit
integer interpreted as an object identifier in storage. The next 8 bytes is an
offset, and the final 8 bytes is a length to read.

With this structure it is possible to resolve any node ID directly to a storage
location. Many nodes may be stored in a single object.
*/

////////////////////////////////////////////////////////////////////////////////

// NodeID is a 24-byte identifier for a node in the nodestore.
type NodeID [24]byte

// Object returns the object identifier of the node.
func (n NodeID) Object() string {
	return strconv.FormatUint(binary.LittleEndian.Uint64(n[:8]), 10)
}

// Offset returns the offset of the node.
func (n NodeID) Offset() uint64 {
	return binary.LittleEndian.Uint64(n[8:])
}

// Length returns the length of the node.
func (n NodeID) Length() uint64 {
	return binary.LittleEndian.Uint64(n[16:])
}

// NewNodeID creates a new node ID from an object ID, offset, and length.
func NewNodeID(oid, offset, length uint64) NodeID {
	var id NodeID
	binary.LittleEndian.PutUint64(id[:8], oid)
	binary.LittleEndian.PutUint64(id[8:], offset)
	binary.LittleEndian.PutUint64(id[16:], length)
	return id
}

// String returns a string representation of the node ID.
func (n NodeID) String() string {
	return fmt.Sprintf("%s:%d:%d", n.Object(), n.Offset(), n.Length())
}

func (n NodeID) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, n.String())), nil
}

func (n *NodeID) UnmarshalJSON(data []byte) error {
	s := string(data)
	s = s[1 : len(s)-1]
	parts := strings.Split(s, ":")
	oid, _ := strconv.ParseUint(parts[0], 10, 64)
	offset, _ := strconv.ParseUint(parts[1], 10, 64)
	length, _ := strconv.ParseUint(parts[2], 10, 64)
	nodeID := generateNodeID(objectID(oid), offset, length)
	*n = nodeID
	return nil
}

// Scan implements the sql.Scanner interface. In SQL storage we store node IDs
// as a string, formated OID:offset:length.
func (n *NodeID) Scan(value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string, got %T", value)
	}
	parts := strings.Split(s, ":")
	oid, _ := strconv.ParseUint(parts[0], 10, 64)
	offset, _ := strconv.ParseUint(parts[1], 10, 64)
	length, _ := strconv.ParseUint(parts[2], 10, 64)
	nodeID := generateNodeID(objectID(oid), offset, length)
	*n = nodeID
	return nil
}

// Value implements the driver.Valuer interface for the NodeID type.
func (n NodeID) Value() (driver.Value, error) {
	return driver.Value(n.String()), nil
}

func generateNodeID(oid objectID, offset uint64, length uint64) NodeID {
	var id NodeID
	binary.LittleEndian.PutUint64(id[:], uint64(oid))
	binary.LittleEndian.PutUint64(id[8:], offset)
	binary.LittleEndian.PutUint64(id[16:], length)
	return id
}
