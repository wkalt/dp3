package mcap

import (
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

type WriterOption func(*mcap.WriterOptions)

func WithCompression(compression mcap.CompressionFormat) WriterOption {
	return func(o *mcap.WriterOptions) {
		o.Compression = compression
	}
}

// NewWriter returns a new mcap writer with sensible defaults.
func NewWriter(w io.Writer, options ...WriterOption) (*mcap.Writer, error) {
	opts := &mcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   4 * megabyte,
		Compression: "zstd",
	}
	for _, opt := range options {
		opt(opts)
	}
	writer, err := mcap.NewWriter(w, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build writer: %w", err)
	}
	return writer, nil
}

// NewReader returns a new mcap reader with sensible defaults.
func NewReader(r io.Reader) (*mcap.Reader, error) {
	reader, err := mcap.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to build reader: %w", err)
	}
	return reader, nil
}

// WriteEmptyFile writes an empty mcap file to the provided writer.
func WriteEmptyFile(w io.Writer) error {
	writer, err := NewWriter(w)
	if err != nil {
		return fmt.Errorf("failed to construct mcap writer: %w", err)
	}
	if err := writer.WriteHeader(&mcap.Header{}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}
