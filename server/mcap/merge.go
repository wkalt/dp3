package mcap

import (
	"container/heap"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"sort"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/util"
)

/*
This file contains two methods related to merging MCAP data streams.

Merge merges two io.Readers onto an io.Writer. The input streams are converted
to mcap readers and the output is wrapped in an MCAP writer.

Nmerge merges multiple MessageIterators onto an io.Writer. There may be any
number of inputs.

Both functions will reassign schema and channel IDs based on hashes of schema
and channel records.
*/

////////////////////////////////////////////////////////////////////////////////

const megabyte = 1024 * 1024

var hashSeed = maphash.MakeSeed() // nolint:gochecknoglobals

// record represents a record in the priority queue.
type record struct {
	schema  *mcap.Schema
	channel *mcap.Channel
	message *mcap.Message
	idx     int
}

// Merge two mcap streams into one. Both streams are assumed to be sorted.
func Merge(writer io.Writer, a, b io.Reader) error {
	var r1, r2 *mcap.Reader
	var it1, it2 MessageIterator
	w, err := NewWriter(writer)
	if err != nil {
		return err
	}
	if r1, err = NewReader(a); err != nil {
		return err
	}
	if r2, err = NewReader(b); err != nil {
		return err
	}
	if it1, err = r1.Messages(mcap.UsingIndex(false), mcap.InOrder(mcap.FileOrder)); err != nil {
		return fmt.Errorf("failed to construct iterator: %w", err)
	}
	if it2, err = r2.Messages(mcap.UsingIndex(false), mcap.InOrder(mcap.FileOrder)); err != nil {
		return fmt.Errorf("failed to construct iterator: %w", err)
	}
	if err := w.WriteHeader(&mcap.Header{}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	var err1, err2 error
	mc := NewMergeCoordinator(w)
	s1, c1, m1, err1 := it1.Next(nil)
	s2, c2, m2, err2 := it2.Next(nil)
	for !errors.Is(err1, io.EOF) || !errors.Is(err2, io.EOF) {
		switch {
		case err1 == nil && errors.Is(err2, io.EOF):
			if err := mc.Write(s1, c1, m1, false); err != nil {
				return err
			}
			s1, c1, m1, err1 = it1.Next(nil)
		case err2 == nil && errors.Is(err1, io.EOF):
			if err := mc.Write(s2, c2, m2, false); err != nil {
				return err
			}
			s2, c2, m2, err2 = it2.Next(nil)
		case m1.LogTime < m2.LogTime:
			if err := mc.Write(s1, c1, m1, false); err != nil {
				return err
			}
			s1, c1, m1, err1 = it1.Next(nil)
		default:
			if err := mc.Write(s2, c2, m2, false); err != nil {
				return err
			}
			s2, c2, m2, err2 = it2.Next(nil)
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}

// Nmerge merges multiple mcap streams into one. All streams are assumed to be
// sorted.
func Nmerge(writer io.Writer, iterators ...MessageIterator) error {
	w, err := NewWriter(writer)
	if err != nil {
		return err
	}
	if err := w.WriteHeader(&mcap.Header{}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	pq := util.NewPriorityQueue[record](func(a, b record) bool {
		if a.message.LogTime == b.message.LogTime {
			return a.message.ChannelID < b.message.ChannelID
		}
		return a.message.LogTime < b.message.LogTime
	})
	heap.Init(pq)
	mc := NewMergeCoordinator(w)

	// push one element from each iterator onto queue
	for i, it := range iterators {
		schema, channel, message, err := it.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return fmt.Errorf("failed to get next message from iterator %d: %w", i, err)
		}
		rec := record{schema, channel, message, i}
		heap.Push(pq, rec)
	}

	for pq.Len() > 0 {
		rec, ok := heap.Pop(pq).(record)
		if !ok {
			return errors.New("failed to pop from priority queue")
		}
		if err := mc.Write(rec.schema, rec.channel, rec.message, false); err != nil {
			return err
		}
		s, c, m, err := iterators[rec.idx].Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return fmt.Errorf("failed to get next message: %w", err)
		}
		heap.Push(pq, record{s, c, m, rec.idx})
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}

func hashBytes(b []byte) uint64 {
	h := maphash.Hash{}
	h.SetSeed(hashSeed)
	_, _ = h.Write(b)
	return h.Sum64()
}

func hashSchema(s *mcap.Schema) uint64 {
	metadata := s.Name + s.Encoding
	return hashBytes(append(s.Data, []byte(metadata)...))
}

func hashChannel(c *mcap.Channel) uint64 {
	content := c.Topic + c.MessageEncoding
	metakeys := make([]string, 0, len(c.Metadata))
	for k := range c.Metadata {
		metakeys = append(metakeys, k)
	}
	sort.Strings(metakeys)
	for _, k := range metakeys {
		content += k + c.Metadata[k]
	}
	content += c.MessageEncoding
	content += c.Topic
	return hashBytes([]byte(content))
}

func initialize(
	writer io.Writer,
	initialized *bool,
) (*mcap.Writer, error) {
	w, err := NewWriter(writer)
	if err != nil {
		return nil, err
	}
	if err := w.WriteHeader(&mcap.Header{}); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}
	*initialized = true
	return w, nil
}
