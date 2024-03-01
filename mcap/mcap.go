package mcap

import (
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

func NewWriter(w io.Writer) (*mcap.Writer, error) {
	return mcap.NewWriter(w, &mcap.WriterOptions{
		IncludeCRC:  true,
		Chunked:     true,
		ChunkSize:   4 * megabyte,
		Compression: "zstd",
	})
}

func NewReader(r io.Reader) (*mcap.Reader, error) {
	return mcap.NewReader(r)
}
