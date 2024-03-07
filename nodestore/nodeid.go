package nodestore

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

type NodeID [16]byte

func (n NodeID) OID() string {
	return strconv.FormatUint(binary.LittleEndian.Uint64(n[:8]), 10)
}

func (n NodeID) Offset() int {
	return int(binary.LittleEndian.Uint32(n[8:]))
}

func (n NodeID) Length() int {
	return int(binary.LittleEndian.Uint32(n[12:]))
}

func (n NodeID) String() string {
	return fmt.Sprintf("%s:%d:%d", n.OID(), n.Offset(), n.Length())
}

func (n *NodeID) Scan(value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string, got %T", value)
	}
	parts := strings.Split(s, ":")
	oid, _ := strconv.ParseUint(parts[0], 10, 64)
	offset, _ := strconv.ParseUint(parts[1], 10, 32)
	length, _ := strconv.ParseUint(parts[2], 10, 32)
	nodeID := generateNodeID(objectID(oid), int(offset), int(length))
	*n = nodeID
	return nil
}

func (n NodeID) Value() (driver.Value, error) {
	return driver.Value(n.String()), nil
}
