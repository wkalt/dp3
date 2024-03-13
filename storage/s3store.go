package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
)

/*
Storage provider for S3-compatible object storage. We use the minio client
library.
*/

////////////////////////////////////////////////////////////////////////////////

const (
	minioErrObjectNotExist = "The specified key does not exist."
)

type s3store struct {
	mc     *minio.Client
	bucket string
}

func NewS3Store(mc *minio.Client, bucket string) *s3store {
	return &s3store{
		mc:     mc,
		bucket: bucket,
	}
}

// Put stores the data in the object store.
func (s *s3store) Put(ctx context.Context, id string, data []byte) error {
	n := int64(len(data))
	_, err := s.mc.PutObject(
		ctx,
		s.bucket,
		id,
		bytes.NewReader(data),
		n,
		minio.PutObjectOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}
	return nil
}

// GetRange retrieves a range of bytes from the object store.
func (s *s3store) GetRange(ctx context.Context, id string, offset int, length int) (io.ReadSeekCloser, error) {
	req := minio.GetObjectOptions{}
	if err := req.SetRange(int64(offset), int64(offset+length)); err != nil {
		return nil, fmt.Errorf("failed to set range: %w", err)
	}
	obj, err := s.mc.GetObject(ctx, s.bucket, id, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	if _, err := obj.Seek(int64(offset), io.SeekStart); err != nil {
		if err.Error() == minioErrObjectNotExist {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("failed to seek: %w", err)
	}
	return obj, nil
}

// Delete removes an object from the object store.
func (s *s3store) Delete(ctx context.Context, id string) error {
	if err := s.mc.RemoveObject(ctx, s.bucket, id, minio.RemoveObjectOptions{}); err != nil {
		if err.Error() == minioErrObjectNotExist {
			return ErrObjectNotFound
		}
		return fmt.Errorf("failed to remove object: %w", err)
	}
	return nil
}

func (s *s3store) String() string {
	return fmt.Sprintf("s3(%s)", s.bucket)
}
