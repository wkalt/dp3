package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

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

// NewDirectoryStore creates a new DirectoryStore. On creation it will scan the
// provided directory for any orphaned .tmp files, which may have been left
// behind on an unclean shutdown. If any are found they are deleted.
func NewDirectoryStore(root string) (*DirectoryStore, error) {
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".tmp") {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("failed to remove %s: %w", path, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to clean up temporary files: %w", err)
	}
	return &DirectoryStore{root: root}, nil
}

// Put stores an object in the directory.
func (d *DirectoryStore) Put(_ context.Context, id string, data []byte) error {
	dir, _ := filepath.Split(d.root + "/" + id)
	if err := util.EnsureDirectoryExists(dir); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	path := d.root + "/" + id
	tmpfile := path + ".tmp"
	if err := os.WriteFile(tmpfile, data, 0600); err != nil {
		return fmt.Errorf("write failure: %w", err)
	}
	if err := os.Rename(tmpfile, path); err != nil {
		return fmt.Errorf("rename failure: %w", err)
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
	path := d.root
	if abspath, err := filepath.Abs(d.root); err == nil {
		path = abspath
	}
	return fmt.Sprintf("directory(%s)", path)
}
