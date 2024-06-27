package util

import "io"

type readSeekNopCloser struct {
	io.ReadSeeker
}

func (r *readSeekNopCloser) Close() error {
	return nil
}

func NewReadSeekNopCloser(rs io.ReadSeeker) io.ReadSeekCloser {
	return &readSeekNopCloser{rs}
}
