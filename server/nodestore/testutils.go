package nodestore

import (
	"crypto/rand"
)

func RandomNodeID() NodeID {
	buf := [24]byte{}
	_, _ = rand.Read(buf[:])
	return NodeID(buf)
}
