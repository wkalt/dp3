package tree

import (
	"fmt"

	"github.com/wkalt/dp3/nodestore"
)

/*
Errors that can be returned by the tree package.
*/

////////////////////////////////////////////////////////////////////////////////

// UnexpectedNodeError is returned when a node of the wrong type is found in the
// nodestore.
type UnexpectedNodeError struct {
	expected nodestore.NodeType
	found    nodestore.Node
}

// Error returns a string representation of the error.
func (e UnexpectedNodeError) Error() string {
	return fmt.Sprintf("expected %s but found %T - database is corrupt", e.expected, e.found)
}

// Is returns true if the target error is an UnexpectedNodeError.
func (e UnexpectedNodeError) Is(target error) bool {
	_, ok := target.(UnexpectedNodeError)
	return ok
}

func NewUnexpectedNodeError(expected nodestore.NodeType, found nodestore.Node) error {
	return UnexpectedNodeError{
		expected: expected,
		found:    found,
	}
}

// OutOfBoundsError is returned when a timestamp is out of range for a node.
type OutOfBoundsError struct {
	t     uint64
	start uint64
	end   uint64
}

// Error returns a string representation of the error.
func (e OutOfBoundsError) Error() string {
	return fmt.Sprintf("timestamp %d out of range [%d, %d)", e.t, e.start, e.end)
}

// Is returns true if the target error is an OutOfBoundsError.
func (e OutOfBoundsError) Is(target error) bool {
	_, ok := target.(OutOfBoundsError)
	return ok
}

// MismatchedHeightsError is returned if a merge is attempted on two nodes of
// different height.
type MismatchedHeightsError struct {
	height1 uint8
	height2 uint8
}

func (e MismatchedHeightsError) Error() string {
	return fmt.Sprintf("mismatched depths: %d and %d", e.height1, e.height2)
}

func (e MismatchedHeightsError) Is(target error) bool {
	_, ok := target.(MismatchedHeightsError)
	return ok
}
