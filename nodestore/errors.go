package nodestore

import "errors"

var ErrNodeNotFound = errors.New("node not found")

var ErrNodeNotStaged = errors.New("node not staged")
