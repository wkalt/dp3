package mcap

import (
	"errors"
	"hash/maphash"
	"io"
	"sort"

	"github.com/foxglove/mcap/go/mcap"
)

const megabyte = 1024 * 1024

func hashBytes(b []byte) uint64 {
	h := maphash.Hash{}
	h.Write(b)
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
	schemaID, ok := c.schemas[schema]
	if !ok {
		// check if we have a matching schema by hash
		schemaHash := hashSchema(schema)
		if mappedID, ok := c.schemaHashes[schemaHash]; ok {
			// associate this schema with the mapped ID
			c.schemas[schema] = mappedID
		} else {
			if err := c.w.WriteSchema(schema); err != nil {
				return err
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
				return err
			}
			c.channels[channel] = c.nextChannelID
			chanID = c.nextChannelID
			c.nextChannelID++
		}
	}
	msg.ChannelID = chanID
	return c.w.WriteMessage(msg)
}

// Merge two mcap streams into one. Both streams are assumed to be sorted.
func Merge(writer io.Writer, a, b io.Reader) error {
	w, err := NewWriter(writer)
	if err != nil {
		return err
	}
	r1, err := NewReader(a)
	if err != nil {
		return err
	}
	r2, err := NewReader(b)
	if err != nil {
		return err
	}
	it1, err := r1.Messages(mcap.UsingIndex(false), mcap.InOrder(mcap.FileOrder))
	if err != nil {
		return err
	}
	it2, err := r2.Messages(mcap.UsingIndex(false), mcap.InOrder(mcap.FileOrder))
	if err != nil {
		return err
	}
	if err := w.WriteHeader(&mcap.Header{}); err != nil {
		return err
	}
	var err1, err2 error
	mc := newCoordinator(w)
	s1, c1, m1, err1 := it1.Next(nil)
	s2, c2, m2, err2 := it2.Next(nil)
	for !errors.Is(err1, io.EOF) || !errors.Is(err2, io.EOF) {
		if err1 == nil && err2 == io.EOF {
			if err := mc.write(s1, c1, m1); err != nil {
				return err
			}
			s1, c1, m1, err1 = it1.Next(nil)
			continue
		}
		if err2 == nil && err1 == io.EOF {
			if err := mc.write(s2, c2, m2); err != nil {
				return err
			}
			s2, c2, m2, err2 = it2.Next(nil)
			continue
		}
		if m1.LogTime < m2.LogTime {
			if err := mc.write(s1, c1, m1); err != nil {
				return err
			}
			s1, c1, m1, err1 = it1.Next(nil)
			continue
		}
		err := mc.write(s2, c2, m2)
		if err != nil {
			return err
		}
		s2, c2, m2, err2 = it2.Next(nil)
	}
	return w.Close()
}
