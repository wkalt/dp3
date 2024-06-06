package tree

/*
 */

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
)

type fileTree struct {
	offset int
	length int

	rootID nodestore.NodeID

	factory func() (io.ReadSeekCloser, error)
}

func NewFileTree(
	factory func() (io.ReadSeekCloser, error),
	offset int,
	length int,
) (*fileTree, error) {
	f, err := factory()
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate reader: %w", err)
	}
	rsc, err := util.NewReadSeekCloserAt(f, offset, length)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate reader at %d:%d: %w", offset, length, err)
	}
	defer rsc.Close()
	_, err = rsc.Seek(-24, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to root ID offset: %w", err)
	}
	rootID := nodestore.NodeID{}
	_, err = io.ReadFull(rsc, rootID[:])
	if err != nil {
		return nil, fmt.Errorf("failed to read root ID: %w", err)
	}
	return &fileTree{factory: factory, offset: offset, length: length, rootID: rootID}, nil
}

func (t *fileTree) Root() nodestore.NodeID {
	return t.rootID
}

func (t *fileTree) Get(ctx context.Context, id nodestore.NodeID) (nodestore.Node, error) {
	f, err := t.factory()
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate reader: %w", err)
	}
	rsc, err := util.NewReadSeekCloserAt(f, t.offset, t.length)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate reader at %d:%d: %w", t.offset, t.length, err)
	}
	defer rsc.Close()

	if _, err = rsc.Seek(int64(id.Offset()), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d: %w", id.Offset(), err)
	}
	value := make([]byte, id.Length())
	if _, err = io.ReadFull(rsc, value); err != nil {
		return nil, fmt.Errorf("failed to read node value: %w", err)
	}
	node, err := nodestore.BytesToNode(value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node: %w", err)
	}
	return node, nil
}

func (t *fileTree) GetLeafNode(ctx context.Context, id nodestore.NodeID) (
	*nodestore.LeafNode,
	io.ReadSeekCloser,
	error,
) {
	f, err := t.factory()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to instantiate reader: %w", err)
	}
	start, err := f.Seek(int64(t.offset)+int64(id.Offset()), io.SeekStart)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to seek to offset %d: %w", id.Offset(), err)
	}

	leafHeaderLength := 1 + 24 + 8 + 8 + 8
	header := make([]byte, leafHeaderLength)
	if _, err = io.ReadFull(f, header); err != nil {
		if err := f.Close(); err != nil {
			return nil, nil, fmt.Errorf("failed to close file: %w", err)
		}
		return nil, nil, fmt.Errorf("failed to read header: %w", err)
	}

	node, err := nodestore.BytesToNode(header)
	if err != nil {
		if err := f.Close(); err != nil {
			return nil, nil, fmt.Errorf("failed to close file: %w", err)
		}
		return nil, nil, fmt.Errorf("failed to parse header: %w", err)
	}
	leaf, ok := node.(*nodestore.LeafNode)
	if !ok {
		if err := f.Close(); err != nil {
			return nil, nil, fmt.Errorf("failed to close file: %w", err)
		}
		return nil, nil, errors.New("not a leaf node")
	}

	// return rsc adjusted to read only the leaf data.
	rsc, err := util.NewReadSeekCloserAt(
		f,
		int(start)+leafHeaderLength,
		int(int(id.Length())-leafHeaderLength),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to instantiate reader: %w", err)
	}

	return leaf, rsc, nil
}
