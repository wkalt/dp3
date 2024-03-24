package wal

import (
	"errors"
	"fmt"

	"github.com/wkalt/dp3/nodestore"
)

// NodeNotFoundError is returned when a node is not found in the WAL.
type NodeNotFoundError struct {
	NodeID nodestore.NodeID
}

func (e NodeNotFoundError) Error() string {
	return fmt.Sprintf("node %s not found", e.NodeID)
}

func (e NodeNotFoundError) Is(target error) bool {
	_, ok := target.(NodeNotFoundError)
	return ok
}

// ErrBadMagic is returned when the WAL magic is not as expected.
var ErrBadMagic = errors.New("bad WAL magic")

// UnsupportedWALError is returned when the WAL version is not supported by the server.
type UnsupportedWALError struct {
	major, minor uint8
}

func (e UnsupportedWALError) Error() string {
	return fmt.Sprintf("unsupported WAL version: %d.%d (current: %d.%d)", e.major, e.minor, currentMajor, currentMinor)
}

func (e UnsupportedWALError) Is(target error) bool {
	_, ok := target.(UnsupportedWALError)
	return ok
}

// CRCMismatchError is returned when the CRC of a record does not match the computed CRC.
type CRCMismatchError struct {
	expected, actual uint32
}

func (e CRCMismatchError) Error() string {
	return fmt.Sprintf("expected CRC %d, got %d", e.expected, e.actual)
}

func (e CRCMismatchError) Is(target error) bool {
	_, ok := target.(CRCMismatchError)
	return ok
}
