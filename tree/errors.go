package tree

import (
	"fmt"

	"github.com/wkalt/dp3/nodestore"
)

type UnexpectedNodeError struct {
	expected nodestore.NodeType
	found    nodestore.Node
}

func (e UnexpectedNodeError) Error() string {
	return fmt.Sprintf("expected %s but found %T - database is corrupt", e.expected, e.found)
}

func (e UnexpectedNodeError) Is(target error) bool {
	_, ok := target.(UnexpectedNodeError)
	return ok
}

func newUnexpectedNodeError(expected nodestore.NodeType, found nodestore.Node) error {
	return UnexpectedNodeError{
		expected: expected,
		found:    found,
	}
}
