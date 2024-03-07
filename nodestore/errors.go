package nodestore

import (
	"errors"
	"fmt"
)

type NodeNotFoundError struct {
	NodeID NodeID
}

func (e NodeNotFoundError) Error() string {
	return fmt.Sprintf("node %s not found", e.NodeID)
}

func (e NodeNotFoundError) Is(target error) bool {
	_, ok := target.(NodeNotFoundError)
	return ok
}

var ErrNodeNotStaged = errors.New("node not staged")
