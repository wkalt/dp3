package treemgr

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"slices"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
)

type writer struct {
	tmgr *treeManager

	lower    uint64
	upper    uint64
	streamID string

	schemas     []*fmcap.Schema
	channels    []*fmcap.Channel
	initialized bool

	buf *bytes.Buffer
	w   *fmcap.Writer

	dims *treeDimensions
}

func newWriter(ctx context.Context, tmgr *treeManager, streamID string) (*writer, error) {
	dims, err := tmgr.dimensions(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("unable to find tree dimensions: %w", err)
	}
	buf := &bytes.Buffer{}
	return &writer{
		tmgr:        tmgr,
		lower:       0,
		upper:       0,
		streamID:    streamID,
		schemas:     []*fmcap.Schema{},
		channels:    []*fmcap.Channel{},
		initialized: false,
		buf:         buf,
		w:           nil,
		dims:        dims,
	}, nil
}

// WriteSchema writes the supplied schema to the output writer.
func (w *writer) WriteSchema(schema *fmcap.Schema) error {
	known := slices.Contains(w.schemas, schema)
	if w.initialized && known {
		return nil
	}
	if w.initialized && !known {
		if err := w.w.WriteSchema(schema); err != nil {
			return fmt.Errorf("failed to write schema: %w", err)
		}
	}
	if !known {
		w.schemas = append(w.schemas, schema)
	}
	return nil
}

func (w *writer) WriteChannel(channel *fmcap.Channel) error {
	known := slices.Contains(w.channels, channel)
	if w.initialized && known {
		return nil
	}
	if w.initialized && !known {
		if err := w.w.WriteChannel(channel); err != nil {
			return fmt.Errorf("failed to write channel: %w", err)
		}
	}
	if !known {
		w.channels = append(w.channels, channel)
	}
	return nil
}

func (w *writer) initialize(ts uint64) (err error) {
	lower, upper := w.dims.bounds(ts)
	w.w, err = mcap.NewWriter(w.buf)
	if err != nil {
		return fmt.Errorf("failed to create mcap writer: %w", err)
	}
	if err := w.w.WriteHeader(&fmcap.Header{}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	w.lower = lower
	w.upper = upper
	for _, schema := range w.schemas {
		if err := w.w.WriteSchema(schema); err != nil {
			return fmt.Errorf("failed to write schema: %w", err)
		}
	}
	for _, channel := range w.channels {
		if err := w.w.WriteChannel(channel); err != nil {
			return fmt.Errorf("failed to write channel: %w", err)
		}
	}
	w.initialized = true
	return nil
}

func (w *writer) flush(ctx context.Context) error {
	if err := w.w.Close(); err != nil {
		return fmt.Errorf("failed to close mcap writer: %w", err)
	}
	if err := w.tmgr.Insert(ctx, w.streamID, w.lower*1e9, w.buf.Bytes()); err != nil {
		return fmt.Errorf("failed to insert %d bytes data for stream %s at time %d: %w",
			w.buf.Len(), w.streamID, w.lower, err)
	}
	w.buf.Reset()
	w.initialized = false
	slog.DebugContext(ctx, "flushed writer", "stream", w.streamID, "lower", w.lower, "upper", w.upper)
	return nil
}

func (w *writer) reset(ctx context.Context, ts uint64) error {
	if err := w.flush(ctx); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}
	if err := w.initialize(ts); err != nil {
		return fmt.Errorf("failed to initialize writer on reset: %w", err)
	}
	return nil
}

func (w *writer) WriteMessage(ctx context.Context, message *fmcap.Message) error {
	if !w.initialized {
		if err := w.initialize(message.LogTime); err != nil {
			return fmt.Errorf("failed to initialize writer: %w", err)
		}
	}
	if message.LogTime < w.lower*1e9 || message.LogTime >= w.upper*1e9 {
		if err := w.reset(ctx, message.LogTime); err != nil {
			return fmt.Errorf("failed to reset writer: %w", err)
		}
	}
	if err := w.w.WriteMessage(message); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	return nil
}

func (w *writer) Close(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}
	return nil
}
