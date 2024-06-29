package mcap

import (
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
)

type WriterOption func(*mcap.WriterOptions)

func WithCompression(compression mcap.CompressionFormat) WriterOption {
	return func(o *mcap.WriterOptions) {
		o.Compression = compression
	}
}

type ZSTDCompressor struct {
	enc *zstd.Encoder
}

func (z *ZSTDCompressor) Compressor() mcap.ResettableWriteCloser {
	return z.enc
}

func (z *ZSTDCompressor) Compression() mcap.CompressionFormat {
	return mcap.CompressionZSTD
}

func NewZSTDCompressor() (mcap.CustomCompressor, error) {
	encoder, err := zstd.NewWriter(
		nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(256*1024),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build new zstd compressor: %w", err)
	}
	return &ZSTDCompressor{enc: encoder}, nil
}

func WithCompressor(compressor mcap.CustomCompressor) WriterOption {
	return func(o *mcap.WriterOptions) {
		o.Compressor = compressor
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

// NewSchema returns a new mcap schema.
func NewSchema(id uint16, name string, encoding string, data []byte) *mcap.Schema {
	return &mcap.Schema{
		ID:       id,
		Name:     name,
		Encoding: encoding,
		Data:     data,
	}
}

// NewChannel returns a new mcap channel.
func NewChannel(
	id uint16,
	schemaID uint16,
	topic string,
	messageEncoding string,
	metadata map[string]string,
) *mcap.Channel {
	return &mcap.Channel{
		ID:              id,
		SchemaID:        schemaID,
		Topic:           topic,
		MessageEncoding: messageEncoding,
		Metadata:        metadata,
	}
}
