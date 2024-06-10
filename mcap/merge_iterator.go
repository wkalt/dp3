package mcap

import (
	"container/heap"
	"errors"
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/util"
)

type mergeIterator struct {
	pq *util.PriorityQueue[record]

	iterators []MessageIterator

	schemaHashes  map[uint64]util.Pair[int, uint16]
	channelHashes map[uint64]util.Pair[int, uint16]

	channels map[util.Pair[int, uint16]]*mcap.Channel
	schemas  map[util.Pair[int, uint16]]*mcap.Schema

	nextSchemaID  uint16
	nextChannelID uint16
}

func (mi *mergeIterator) remap(
	idx int,
	schema *mcap.Schema,
	channel *mcap.Channel,
	message *mcap.Message,
) (*mcap.Schema, *mcap.Channel, *mcap.Message) {
	schemaKey := util.NewPair(idx, schema.ID)
	newSchema, ok := mi.schemas[schemaKey]
	if !ok {
		schemaHash := hashSchema(schema)
		if mapped, ok := mi.schemaHashes[schemaHash]; ok {
			mi.schemas[schemaKey] = mi.schemas[mapped]
			newSchema = mi.schemas[mapped]
		} else {
			schemaID := mi.nextSchemaID
			newSchema = NewSchema(schemaID, schema.Name, schema.Encoding, schema.Data)
			mi.schemas[schemaKey] = schema
			mi.schemaHashes[schemaHash] = schemaKey
			mi.nextSchemaID++
		}
	}
	channelKey := util.NewPair(idx, channel.ID)
	newChannel, ok := mi.channels[channelKey]
	if !ok {
		channelHash := hashChannel(channel)
		if mapped, ok := mi.channelHashes[channelHash]; ok {
			mi.channels[channelKey] = mi.channels[mapped]
			newChannel = mi.channels[mapped]
		} else {
			newChannel = &mcap.Channel{
				ID:              mi.nextChannelID,
				SchemaID:        newSchema.ID,
				Topic:           channel.Topic,
				MessageEncoding: channel.MessageEncoding,
				Metadata:        channel.Metadata,
			}
			mi.channels[channelKey] = newChannel
			mi.channelHashes[channelHash] = channelKey
			mi.nextChannelID++
		}
	}
	message.ChannelID = newChannel.ID
	return newSchema, newChannel, message
}

func (mi *mergeIterator) Next([]byte) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	if mi.pq.Len() == 0 {
		return nil, nil, nil, io.EOF
	}
	rec, ok := heap.Pop(mi.pq).(record)
	if !ok {
		return nil, nil, nil, errors.New("failed to pop from priority queue")
	}
	s, c, m, err := mi.iterators[rec.idx].Next(nil)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, nil, nil, fmt.Errorf("failed to pull next message: %w", err)
	}

	if err == nil {
		rec := record{s, c, m, rec.idx}
		heap.Push(mi.pq, rec)
	}
	s2, c2, m2 := mi.remap(rec.idx, rec.schema, rec.channel, rec.message)
	return s2, c2, m2, nil
}

func NmergeIterator(descending bool, iterators ...MessageIterator) (MessageIterator, error) {
	pq := util.NewPriorityQueue(func(a, b record) bool {
		if a.message.LogTime == b.message.LogTime {
			return a.message.ChannelID < b.message.ChannelID
		}
		if descending {
			return a.message.LogTime > b.message.LogTime
		}
		return a.message.LogTime < b.message.LogTime
	})
	heap.Init(pq)

	// push one element from each iterator onto queue
	for i, it := range iterators {
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

	return &mergeIterator{
		pq:            pq,
		iterators:     iterators,
		schemaHashes:  map[uint64]util.Pair[int, uint16]{},
		channelHashes: map[uint64]util.Pair[int, uint16]{},
		channels:      map[util.Pair[int, uint16]]*mcap.Channel{},
		schemas:       map[util.Pair[int, uint16]]*mcap.Schema{},
		nextSchemaID:  1,
		nextChannelID: 0,
	}, nil
}
