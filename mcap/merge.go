package mcap

import (
	"container/heap"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"sort"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/util"
)

const megabyte = 1024 * 1024

func hashBytes(b []byte) uint64 {
	h := maphash.Hash{}
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

type coordinator struct {
	w             *mcap.Writer
	schemaHashes  map[uint64]uint16
	channelHashes map[uint64]uint16
	schemas       map[*mcap.Schema]uint16
	channels      map[*mcap.Channel]uint16
	nextSchemaID  uint16
	nextChannelID uint16
}

func newCoordinator(w *mcap.Writer) *coordinator {
	return &coordinator{
		w:             w,
		schemaHashes:  make(map[uint64]uint16),
		channelHashes: make(map[uint64]uint16),
		schemas:       make(map[*mcap.Schema]uint16),
		channels:      make(map[*mcap.Channel]uint16),
	}
}

func (c *coordinator) write(schema *mcap.Schema, channel *mcap.Channel, msg *mcap.Message) error {
	if schema == nil {
		return errors.New("schema is nil")
	}
	schemaID, ok := c.schemas[schema]
	if !ok {
		// check if we have a matching schema by hash
		schemaHash := hashSchema(schema)
		if mappedID, ok := c.schemaHashes[schemaHash]; ok {
			// associate this schema with the mapped ID
			c.schemas[schema] = mappedID
		} else {
			if err := c.w.WriteSchema(schema); err != nil {
				return fmt.Errorf("failed to write schema: %w", err)
			}
			c.schemas[schema] = c.nextSchemaID
			schemaID = c.nextSchemaID
			c.nextSchemaID++
		}
	}
	chanID, ok := c.channels[channel]
	if !ok {
		// check if we have a matching channel by hash
		channelHash := hashChannel(channel)
		if mappedID, ok := c.channelHashes[channelHash]; ok {
			// associate this channel with the mapped ID
			c.channels[channel] = mappedID
			chanID = mappedID
		} else {
			newChannel := &mcap.Channel{
				ID:              c.nextChannelID,
				SchemaID:        schemaID,
				Topic:           channel.Topic,
				MessageEncoding: channel.MessageEncoding,
				Metadata:        channel.Metadata,
			}
			if err := c.w.WriteChannel(newChannel); err != nil {
				return fmt.Errorf("failed to write channel: %w", err)
			}
			c.channels[channel] = c.nextChannelID
			chanID = c.nextChannelID
			c.nextChannelID++
		}
	}
	msg.ChannelID = chanID
	if err := c.w.WriteMessage(msg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	return nil
}

type record struct {
	schema  *mcap.Schema
	channel *mcap.Channel
	message *mcap.Message
	idx     int
}

func Nmerge(writer io.Writer, iterators ...mcap.MessageIterator) error {
	w, err := NewWriter(writer)
	if err != nil {
		return err
	}
	if err := w.WriteHeader(&mcap.Header{}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	pq := util.NewPriorityQueue[record, uint64]()
	heap.Init(pq)
	mc := newCoordinator(w)

	// push one element from each iterator onto queue
	for i, it := range iterators {
		schema, channel, message, err := it.Next(nil)
		if err != nil {
			return fmt.Errorf("failed to get next message from iterator %d: %w", i, err)
		}
		var item = util.Item[record, uint64]{
			Value:    record{schema, channel, message, i},
			Priority: message.LogTime,
		}
		heap.Push(pq, &item)
	}

	for pq.Len() > 0 {
		rec := heap.Pop(pq).(record)
		if err := mc.write(rec.schema, rec.channel, rec.message); err != nil {
			return err
		}
		schema, channel, message, err := iterators[rec.idx].Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return err
		}
		var item = util.Item[record, uint64]{
			Value:    record{schema, channel, message, rec.idx},
			Priority: message.LogTime,
		}
		heap.Push(pq, &item)
	}
	return w.Close()
}

// Merge two mcap streams into one. Both streams are assumed to be sorted.
func Merge(writer io.Writer, a, b io.Reader) error {
	var r1, r2 *mcap.Reader
	var it1, it2 mcap.MessageIterator
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
	mc := newCoordinator(w)
	s1, c1, m1, err1 := it1.Next(nil)
	s2, c2, m2, err2 := it2.Next(nil)
	for !errors.Is(err1, io.EOF) || !errors.Is(err2, io.EOF) {
		switch {
		case err1 == nil && errors.Is(err2, io.EOF):
			if err := mc.write(s1, c1, m1); err != nil {
				return err
			}
			s1, c1, m1, err1 = it1.Next(nil)
		case err2 == nil && errors.Is(err1, io.EOF):
			if err := mc.write(s2, c2, m2); err != nil {
				return err
			}
			s2, c2, m2, err2 = it2.Next(nil)
		case m1.LogTime < m2.LogTime:
			if err := mc.write(s1, c1, m1); err != nil {
				return err
			}
			s1, c1, m1, err1 = it1.Next(nil)
		default:
			if err := mc.write(s2, c2, m2); err != nil {
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
