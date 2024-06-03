package util

import (
	"fmt"
	"io"
)

type CountingWriter struct {
	w io.Writer
	n int
}

func (c *CountingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += n
	if err != nil {
		return n, fmt.Errorf("write failure: %w", err)
	}
	return n, nil
}

func (c *CountingWriter) Count() int {
	return c.n
}

func NewCountingWriter(w io.Writer) *CountingWriter {
	return &CountingWriter{w: w}
}
