package mcap

import (
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

/*
The MCAP message iterator requires an instantiated mcap.Reader, and
instantiating the Reader does IO to verify the magic number. When we build an
execution tree of iterators we don't want to do any IO until the first call to
next, to avoid having more than the necessary number of file descriptors open at
once. So this iterator wraps the mcap reader with lazy initialization.
*/

////////////////////////////////////////////////////////////////////////////////

type lazyIndexedIterator struct {
	it    mcap.MessageIterator
	rsc   io.ReadSeekCloser
	start uint64
	end   uint64

	initialized bool
}

func (it *lazyIndexedIterator) initialize() error {
	reader, err := NewReader(it.rsc)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	iterator, err := reader.Messages(mcap.AfterNanos(it.start), mcap.BeforeNanos(it.end))
	if err != nil {
		return fmt.Errorf("failed to create message iterator: %w", err)
	}
	it.it = iterator
	it.initialized = true
	return nil
}

// Next returns the next message in the iterator.
func (it *lazyIndexedIterator) Next(_ []byte) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	if !it.initialized {
		if err := it.initialize(); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to initialize iterator: %w", err)
		}
	}
	s, c, m, err := it.it.Next(nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get next message: %w", err)
	}
	return s, c, m, nil
}

// NewLazyIndexedIterator returns an MCAP message iterator that is not
// initialized (doing IO) until the first call to Next.
func NewLazyIndexedIterator(rsc io.ReadSeekCloser, start uint64, end uint64) mcap.MessageIterator {
	return &lazyIndexedIterator{rsc: rsc, start: start, end: end}
}
