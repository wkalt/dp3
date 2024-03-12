package treemgr

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util/log"
)

/*
The tree writer is responsible for transforming input MCAP data streams into
properly-portioned leaf nodes, according to the timestamps of the messages, and
then inserting them with tmgr.insert.

Messages coming into the writer are mostly-ordered. Probably a good guess is
99.999% ordered, within a single import task. If we do get an out of order
message, we can end up in a situation where we write to the WAL, [1 5], [6 10],
[3 5], with the first and third writes destined for the same leaf (and the first
is out the door already - in the WAL).

Writes to tmgr go to the WAL, not to permanent storage. When the ultimate flush
to permanent storage occurs, the two leaves with data for the same range in this
scenario will be merged into one.
*/

////////////////////////////////////////////////////////////////////////////////

type writer struct {
	tmgr *TreeManager

	lower uint64
	upper uint64

	producerID string
	topic      string

	schemas     []*fmcap.Schema
	channels    []*fmcap.Channel
	initialized bool

	buf *bytes.Buffer
	w   *fmcap.Writer

	dims       *treeDimensions
	statistics *nodestore.Statistics
}

func newWriter(ctx context.Context, tmgr *TreeManager, producerID string, topic string) (*writer, error) {
	dims, err := tmgr.dimensions(ctx, producerID, topic)
	if err != nil {
		return nil, fmt.Errorf("unable to find tree dimensions: %w", err)
	}
	buf := &bytes.Buffer{}
	return &writer{
		tmgr:        tmgr,
		lower:       0,
		upper:       0,
		schemas:     []*fmcap.Schema{},
		channels:    []*fmcap.Channel{},
		initialized: false,
		buf:         buf,
		w:           nil,
		dims:        dims,

		producerID: producerID,
		topic:      topic,
		statistics: &nodestore.Statistics{},
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
	w.statistics = &nodestore.Statistics{}
	return nil
}

func (w *writer) flush(ctx context.Context) error {
	if err := w.w.Close(); err != nil {
		return fmt.Errorf("failed to close mcap writer: %w", err)
	}
	if err := w.tmgr.insert(ctx, w.producerID, w.topic, w.lower*1e9, w.buf.Bytes(), w.statistics); err != nil {
		return fmt.Errorf("failed to insert %d bytes data for stream %s/%s at time %d: %w",
			w.buf.Len(), w.producerID, w.topic, w.lower, err)
	}
	w.buf.Reset()
	w.initialized = false
	log.Debugw(ctx, "flushed writer",
		"producer_id", w.producerID,
		"topic", w.topic,
		"lower", w.lower,
		"upper", w.upper)
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

func (w *writer) updateStatistics(_ *fmcap.Message) {
	w.statistics.MessageCount++
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
	w.updateStatistics(message)
	return nil
}

func (w *writer) Close(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}
	return nil
}
