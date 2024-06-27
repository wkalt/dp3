package nodestore

import (
	"errors"
	"fmt"
)

type NodeNotFoundError struct {
	Prefix string
	NodeID NodeID
}

func (e NodeNotFoundError) Error() string {
	return fmt.Sprintf("node %s/%s not found", e.Prefix, e.NodeID)
}

func (e NodeNotFoundError) Is(target error) bool {
	_, ok := target.(NodeNotFoundError)
	return ok
}

var ErrNodeNotStaged = errors.New("node not staged")
