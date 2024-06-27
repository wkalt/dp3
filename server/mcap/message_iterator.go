package mcap

import (
	"errors"
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

/*
This file describes the MessageIterator interface and contains generic methods
for iterating over mcap messages.
*/

////////////////////////////////////////////////////////////////////////////////

// MessageIterator describes an iterator over mcap messages.
type MessageIterator interface {
	Next(buf []byte) (*mcap.Schema, *mcap.Channel, *mcap.Message, error)
}

// SerializeIterator writes messages from a message iterator into a writer,
// formatted as MCAP. The closeEmpty flag determines whether the writer should
// be closed with an empty file if no messages are written.
func SerializeIterator(
	writer io.Writer,
	iterator MessageIterator,
	msgCallback func(*mcap.Schema, *mcap.Channel, *mcap.Message) error,
) error {
	var w *mcap.Writer
	var initialized bool
	schemas := make(map[uint16]bool)
	channels := make(map[uint16]bool)
	for {
		s, c, m, err := iterator.Next(nil) // todo: reuse buffer
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				if !initialized {
					if w, err = initialize(writer, &initialized); err != nil {
						return err
					}
				}
				if err := w.Close(); err != nil {
					return fmt.Errorf("failed to close writer: %w", err)
				}
				return nil
			default:
				return fmt.Errorf("failed to get next message: %w", err)
			}
		}
		if !initialized {
			w, err = initialize(writer, &initialized)
			if err != nil {
				return err
			}
		}
		if _, ok := schemas[s.ID]; !ok {
			if err := w.WriteSchema(s); err != nil {
				return fmt.Errorf("failed to write schema: %w", err)
			}
			schemas[s.ID] = true
		}
		if _, ok := channels[c.ID]; !ok {
			if err := w.WriteChannel(c); err != nil {
				return fmt.Errorf("failed to write channel: %w", err)
			}
			channels[c.ID] = true
		}
		if err := w.WriteMessage(m); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
		if msgCallback != nil {
			if err := msgCallback(s, c, m); err != nil {
				return fmt.Errorf("failed to call message callback: %w", err)
			}
		}
	}
}
