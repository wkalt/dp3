package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
)

/*
ByteTree is a treereader implementation backed by the byte serialization of a
memtree. The memtree uses it to deserialize its on-disk representation from WAL.
*/

////////////////////////////////////////////////////////////////////////////////

type byteTree struct{ r *bytes.Reader }

func isLeaf(data []byte) bool {
	return data[0] > 128
}

func (b *byteTree) GetReader(ctx context.Context, id nodestore.NodeID) (io.ReadSeekCloser, error) {
	offset := id.Offset()
	length := id.Length()
	_, err := b.r.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to offset: %w", err)
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(b.r, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read node data: %w", err)
	}
	return util.NewReadSeekNopCloser(bytes.NewReader(buf[1:])), nil
}

func (b *byteTree) Get(ctx context.Context, id nodestore.NodeID) (nodestore.Node, error) {
	offset := id.Offset()
	length := id.Length()
	_, err := b.r.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to offset: %w", err)
	}
	buf := make([]byte, length)
	n, err := io.ReadFull(b.r, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read node data: %w", err)
	}
	if n < 1 {
		return nil, errors.New("node data is empty")
	}
	if isLeaf(buf) {
		node := nodestore.NewLeafNode(nil, nil, nil, nil)
		if err := node.FromBytes(buf); err != nil {
			return nil, fmt.Errorf("failed to deserialize leaf node: %w", err)
		}
		return node, nil
	}
	node := nodestore.NewInnerNode(0, 0, 0, 0)
	if err := node.FromBytes(buf); err != nil {
		return nil, fmt.Errorf("failed to deserialize inner node: %w", err)
	}
	return node, nil
}

func (b *byteTree) Put(ctx context.Context, id nodestore.NodeID, node nodestore.Node) error {
	return errors.New("not implemented")
}

func (b *byteTree) Root() nodestore.NodeID {
	_, _ = b.r.Seek(-24, io.SeekEnd)
	id := make([]byte, 24)
	_, _ = io.ReadFull(b.r, id)
	return nodestore.NodeID(id)
}

func (b *byteTree) SetRoot(nodestore.NodeID) {
	panic("not implemented")
}
