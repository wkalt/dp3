package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/wkalt/dp3/util"
)

/*
DirectoryStore is a simple storage provider that stores objects in a local
directory. It is not suitable for production use.
*/

////////////////////////////////////////////////////////////////////////////////

type DirectoryStore struct {
	root string
}

// NewDirectoryStore creates a new DirectoryStore.
func NewDirectoryStore(root string) *DirectoryStore {
	return &DirectoryStore{root: root}
}

// Put stores an object in the directory.
func (d *DirectoryStore) Put(_ context.Context, id string, data []byte) error {
	err := os.WriteFile(d.root+"/"+id, data, 0600)
	if err != nil {
		return fmt.Errorf("write failure: %w", err)
	}
	return nil
}

// GetRange retrieves a range of bytes from an object in the directory.
func (d *DirectoryStore) GetRange(_ context.Context, id string, offset int, length int) (io.ReadSeekCloser, error) {
	f, err := os.Open(d.root + "/" + id)
	if err != nil {
		return nil, ErrObjectNotFound
	}
	defer f.Close()
	_, err = f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek failure: %w", err)
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(f, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return util.NewReadSeekNopCloser(bytes.NewReader(buf)), nil
}

// Delete removes an object from the directory.
func (d *DirectoryStore) Delete(_ context.Context, id string) error {
	err := os.Remove(d.root + "/" + id)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) { // For conformance to S3 API
			return nil
		}
		return fmt.Errorf("deletion failure: %w", err)
	}
	return nil
}

func (d *DirectoryStore) String() string {
	return fmt.Sprintf("directory(%s)", d.root)
}
