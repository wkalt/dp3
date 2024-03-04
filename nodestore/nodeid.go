package nodestore

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
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
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("failed to decode hex: %w", err)
	}

	copy(n[:], bytes)
	return nil
}

func (n NodeID) Value() (driver.Value, error) {
	return driver.Value(hex.EncodeToString(n[:])), nil
}
