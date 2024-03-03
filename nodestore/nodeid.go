package nodestore

import (
	"encoding/binary"
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
