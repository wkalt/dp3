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
	rsc, err := util.NewReadSeekCloserAt(
		f,
		t.offset+int(id.Offset()),
		int(id.Length()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to instantiate reader: %w", err)
	}
	leafHeaderLength := 1 + 24 + 8 + 8 + 8
	header := make([]byte, leafHeaderLength)
	if _, err = io.ReadFull(rsc, header); err != nil {
		rsc.Close()
		return nil, nil, fmt.Errorf("failed to read header: %w", err)
	}
	node, err := nodestore.BytesToNode(header)
	if err != nil {
		rsc.Close()
		return nil, nil, fmt.Errorf("failed to parse header: %w", err)
	}
	leaf, ok := node.(*nodestore.LeafNode)
	if !ok {
		rsc.Close()
		return nil, nil, errors.New("not a leaf node")
	}
	return leaf, rsc, nil
}
