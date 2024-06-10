package mcap

import (
	"container/heap"
	"errors"
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/util"
)

/*
The filter merge iterator is a message iterator implementation that merges
multiple iterators while deduplicating messages based on timestamp/sequence, and
also accepting an optional "filter" iterator that may be used to mask matching
messages.

For example, suppose the input iterators are:

[(1, 1), (2, 2), (3, 3)]
[(1, 1), (3, 3), (4, 4)]

The output iterator would be:
[(1, 1), (2, 2), (3, 3), (4, 4)]

If a mask were supplied, and the mask were equal to:
[(1, 1), (3, 3), (4, 4)]

then the output would be:
[(2, 2)]


We use this iterator for deduplicating new data against concurrent inserts, as
well as data that is already present in the tree.
*/

////////////////////////////////////////////////////////////////////////////////

type filterMergeIterator struct {
	pq *util.PriorityQueue[record]

	iterators []MessageIterator

	schemaHashes  map[uint64]*mcap.Schema
	channelHashes map[uint64]*mcap.Channel

	schemas  map[*mcap.Schema]*mcap.Schema
	channels map[*mcap.Channel]*mcap.Channel

	nextSchemaID  uint16
	nextChannelID uint16

	lastMark marker
}

// NewFilterMergeIterator returns a new filter merge iterator.
func NewFilterMergeIterator(
	filter MessageIterator,
	iterators ...MessageIterator,
) (MessageIterator, error) {
	pq := util.NewPriorityQueue(func(a, b record) bool {
		if a.message.LogTime == b.message.LogTime {
			if a.message.Sequence == b.message.Sequence {
				return a.idx < b.idx // zeroth IDX is the mask, and always precedes dupes
			}
			return a.message.Sequence < b.message.Sequence
		}
		return a.message.LogTime < b.message.LogTime
	})
	heap.Init(pq)

	// NB: mask may be nil, in which case zeroth index is unused.
	targets := []MessageIterator{filter}
	targets = append(targets, iterators...)

	// push one element from each iterator onto queue
	for i, it := range targets {
		if it == nil {
			continue
		}
		schema, channel, message, err := it.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return nil, fmt.Errorf("failed to get next message from iterator %d: %w", i, err)
		}
		rec := record{schema, channel, message, i}
		heap.Push(pq, rec)
	}

	return &filterMergeIterator{
		pq:            pq,
		iterators:     targets,
		schemaHashes:  make(map[uint64]*mcap.Schema),
		channelHashes: make(map[uint64]*mcap.Channel),
		channels:      make(map[*mcap.Channel]*mcap.Channel),
		schemas:       make(map[*mcap.Schema]*mcap.Schema),
		nextSchemaID:  1,
		nextChannelID: 0,
	}, nil
}

// FilterMerge merges a set of iterators and optional mask onto an output writer
// in MCAP format.
func FilterMerge(
	w io.Writer,
	msgCallback func(*mcap.Schema, *mcap.Channel, *mcap.Message) error,
	mask MessageIterator,
	iterators ...MessageIterator,
) error {
	iterator, err := NewFilterMergeIterator(mask, iterators...)
	if err != nil {
		return err
	}
	return SerializeIterator(w, iterator, msgCallback)
}

// Next returns the next message from the iterator.
func (mi *filterMergeIterator) Next([]byte) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	for mi.pq.Len() > 0 {
		rec, ok := heap.Pop(mi.pq).(record)
		if !ok {
			return nil, nil, nil, errors.New("failed to pop from priority queue")
		}

		s, c, m, err := mi.iterators[rec.idx].Next(nil)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, nil, nil, fmt.Errorf("failed to pull next message: %w", err)
		}

		// if we got no error, then we got no EOF, which means these structures
		// are populated and we can push them onto the heap.
		if err == nil {
			rec := record{s, c, m, rec.idx}
			heap.Push(mi.pq, rec)
		}

		// Skip any messages from the mask, but record the time/sequence
		if rec.idx == 0 {
			mi.lastMark.timestamp = rec.message.LogTime
			mi.lastMark.sequence = rec.message.Sequence
			mi.lastMark.valid = true
			continue
		}

		// If the message matches the last mark's timestamp and sequence, skip
		// it as a dupe.
		if mi.lastMark.valid && rec.message.LogTime == mi.lastMark.timestamp &&
			rec.message.Sequence == mi.lastMark.sequence {
			continue
		}

		// The message is not a dupe and is not from the mask. Set the last mark
		// so any subsequent dupe is removed, remap the channel/schema IDs, and
		// return it.
		mi.lastMark.timestamp = rec.message.LogTime
		mi.lastMark.sequence = rec.message.Sequence
		mi.lastMark.valid = true

		s2, c2, m2 := mi.remap(rec.schema, rec.channel, rec.message)
		return s2, c2, m2, nil
	}
	return nil, nil, nil, io.EOF
}

// remap remaps the schema and channel IDs of a message from one of the merge
// inputs, to a coherent assignment of output IDs.
func (mi *filterMergeIterator) remap(
	schema *mcap.Schema,
	channel *mcap.Channel,
	message *mcap.Message,
) (s *mcap.Schema, c *mcap.Channel, m *mcap.Message) {
	var schemaID, channelID uint16
	var ok bool
	if s, ok = mi.schemas[schema]; !ok {
		schemaHash := hashSchema(schema)
		if mapped, ok := mi.schemaHashes[schemaHash]; ok {
			mi.schemas[schema] = mapped
			s = mapped
		} else {
			schemaID = mi.nextSchemaID
			s = NewSchema(schemaID, schema.Name, schema.Encoding, schema.Data)
			mi.schemas[schema] = s
			mi.schemaHashes[schemaHash] = s
			mi.nextSchemaID++
		}
	}

	if c, ok = mi.channels[channel]; !ok {
		channelHash := hashChannel(channel)
		if mapped, ok := mi.channelHashes[channelHash]; ok {
			mi.channels[channel] = mapped
			c = mapped
		} else {
			channelID = mi.nextChannelID
			c = NewChannel(channelID, schemaID, channel.Topic,
				channel.MessageEncoding, channel.Metadata,
			)
			mi.channels[channel] = c
			mi.channelHashes[channelHash] = c
			mi.nextChannelID++
		}
	}
	message.ChannelID = channelID
	return s, c, message
}

// marker represents state used to track the last-seen message.
type marker struct {
	timestamp uint64
	sequence  uint32
	valid     bool
}
