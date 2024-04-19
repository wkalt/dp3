package mcap

import (
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/foxglove/mcap/go/mcap"
)

/*
A concat iterator returns the concatenation of multiple iterators. It is up to
the caller to ensure that the inputs are in the correct order and
nonoverlapping.
*/

//////////////////////////////////////////////////////////////////////////////

type concatIterator struct {
	iterators []mcap.MessageIterator
	idx       int
	rs        io.ReadSeeker
}

// Next returns the next message in the iterator.
func (ci *concatIterator) Next(buf []byte) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	if ci.idx >= len(ci.iterators) {
		return nil, nil, nil, io.EOF
	}
	schema, channel, message, err := ci.iterators[ci.idx].Next(buf)
	if err == nil {
		return schema, channel, message, nil
	}
	if errors.Is(err, io.EOF) {
		_, err := ci.rs.Seek(0, io.SeekStart)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to seek to reader start: %w", err)
		}
		ci.idx++
		return ci.Next(buf)
	}
	return nil, nil, nil, fmt.Errorf("failed to read next message: %w", err)
}

// NewConcatIterator returns a new MessageIterator that concatenates the messages
// from the given ranges.
func NewConcatIterator(rs io.ReadSeeker, ranges [][]uint64, descending bool) mcap.MessageIterator {
	iterators := make([]mcap.MessageIterator, 0, len(ranges))
	if descending {
		slices.Reverse(ranges)
	}
	for _, r := range ranges {
		iterators = append(iterators, NewLazyIndexedIterator(rs, r[0], r[1], descending))
	}
	return &concatIterator{rs: rs, iterators: iterators}
}
