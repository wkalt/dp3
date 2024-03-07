package storage

import (
	"context"
	"fmt"
	"io"
	"os"
)

type DirectoryStore struct {
	root string
}

func NewDirectoryStore(root string) *DirectoryStore {
	return &DirectoryStore{root: root}
}

func (d *DirectoryStore) Put(_ context.Context, id string, data []byte) error {
	err := os.WriteFile(d.root+"/"+id, data, 0600)
	if err != nil {
		return fmt.Errorf("write failure: %w", err)
	}
	return nil
}

func (d *DirectoryStore) GetRange(_ context.Context, id string, offset int, length int) ([]byte, error) {
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
	return buf, nil
}

func (d *DirectoryStore) Delete(_ context.Context, id string) error {
	err := os.Remove(d.root + "/" + id)
	if err != nil {
		return fmt.Errorf("deletion failure: %w", err)
	}
	return nil
}
