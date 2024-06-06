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

var hashSeed = maphash.MakeSeed() // nolint:gochecknoglobals

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

type MergeCoordinator struct {
	w             *mcap.Writer
	schemaHashes  map[uint64]uint16
	channelHashes map[uint64]uint16
	schemas       map[*mcap.Schema]uint16
	channels      map[*mcap.Channel]uint16
	nextSchemaID  uint16
	nextChannelID uint16
}

func NewMergeCoordinator(w *mcap.Writer) *MergeCoordinator {
	return &MergeCoordinator{
		w:             w,
		schemaHashes:  make(map[uint64]uint16),
		channelHashes: make(map[uint64]uint16),
		schemas:       make(map[*mcap.Schema]uint16),
		channels:      make(map[*mcap.Channel]uint16),

		nextSchemaID: 1,
	}
}

func (c *MergeCoordinator) WriteMetadata(metadata *mcap.Metadata) error {
	if err := c.w.WriteMetadata(metadata); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}
	return nil
}

func (c *MergeCoordinator) handleSchema(skeleton bool, schema *mcap.Schema) (uint16, error) {
	var schemaID uint16
	// check if we have a matching schema by hash
	schemaHash := hashSchema(schema)
	if mappedID, ok := c.schemaHashes[schemaHash]; ok {
		// associate this schemau with the mapped ID
		c.schemas[schema] = mappedID
		schemaID = mappedID
	} else {
		schemaID = c.nextSchemaID
		newSchema := &mcap.Schema{
			ID:       schemaID,
			Name:     schema.Name,
			Encoding: schema.Encoding,
			Data:     util.When(!skeleton, schema.Data, []byte{}),
		}
		if err := c.w.WriteSchema(newSchema); err != nil {
			return schemaID, fmt.Errorf("failed to write schema: %w", err)
		}
		c.schemaHashes[schemaHash] = schemaID
		c.schemas[schema] = schemaID
		c.nextSchemaID++
	}
	return schemaID, nil
}

func (c *MergeCoordinator) handleChannel(schemaID uint16, channel *mcap.Channel) (uint16, error) {
	var chanID uint16
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
			return chanID, fmt.Errorf("failed to write channel: %w", err)
		}
		c.channels[channel] = c.nextChannelID
		c.channelHashes[channelHash] = c.nextChannelID
		chanID = c.nextChannelID
		c.nextChannelID++
	}
	return chanID, nil
}

func (c *MergeCoordinator) Write(
	schema *mcap.Schema,
	channel *mcap.Channel,
	msg *mcap.Message,
	skeleton bool,
) error {
	var err error
	schemaID, ok := c.schemas[schema]
	if !ok {
		if schemaID, err = c.handleSchema(skeleton, schema); err != nil {
			return fmt.Errorf("failed to handle schema: %w", err)
		}
	}
	chanID, ok := c.channels[channel]
	if !ok {
		if chanID, err = c.handleChannel(schemaID, channel); err != nil {
			return fmt.Errorf("failed to handle channel: %w", err)
		}
	}
	msg.ChannelID = chanID
	if skeleton {
		msg.Data = []byte{}
	}
	if err := c.w.WriteMessage(msg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	return nil
}

func (c *MergeCoordinator) Close() error {
	if err := c.w.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}

type record struct {
	schema  *mcap.Schema
	channel *mcap.Channel
	message *mcap.Message
	idx     int
}

func initialize(
	writer io.Writer,
	onInit func() error,
	initialized *bool,
) (*mcap.Writer, error) {
	if err := onInit(); err != nil {
		return nil, err
	}
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

var ErrNoOutput = errors.New("no output")

func SerializeIterator(
	writer io.Writer,
	iterator MessageIterator,
	onInit func() error,
	closeEmpty bool,
	msgCallback func(*mcap.Schema, *mcap.Channel, *mcap.Message) error,
) error {
	var w *mcap.Writer
	var initialized bool
	schemas := make(map[uint16]bool)
	channels := make(map[uint16]bool)
	for {
		s, c, m, err := iterator.Next(nil) // todo: reuse buffer
		if err != nil {
			if errors.Is(err, io.EOF) {
				if !initialized && closeEmpty {
					w, err = initialize(writer, onInit, &initialized)
					if err != nil {
						return err
					}
				}

				// If we haven't initialized, there is nothing to close. No data
				// was written.
				if !initialized {
					return ErrNoOutput
				}

				return w.Close()
			}
			return fmt.Errorf("failed to get next message: %w", err)
		}
		if !initialized {
			w, err = initialize(writer, onInit, &initialized)
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
