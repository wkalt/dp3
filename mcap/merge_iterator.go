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
	pq *util.PriorityQueue[record, uint64]

	iterators []mcap.MessageIterator

	schemaHashes  map[uint64]numkey
	channelHashes map[uint64]numkey

	channels map[numkey]*mcap.Channel
	schemas  map[numkey]*mcap.Schema

	nextSchemaID  uint16
	nextChannelID uint16
}

type numkey struct {
	idx int
	id  uint16
}

func (mi *mergeIterator) remap(
	idx int,
	schema *mcap.Schema,
	channel *mcap.Channel,
	message *mcap.Message,
) (*mcap.Schema, *mcap.Channel, *mcap.Message) {
	skey := numkey{idx, schema.ID}
	newSchema, ok := mi.schemas[skey]
	if !ok {
		schemaHash := hashSchema(schema)
		if mapped, ok := mi.schemaHashes[schemaHash]; ok {
			mi.schemas[skey] = mi.schemas[mapped]
			newSchema = mi.schemas[mapped]
		} else {
			schemaID := mi.nextSchemaID
			newSchema = &mcap.Schema{
				ID:       schemaID,
				Name:     schema.Name,
				Encoding: schema.Encoding,
				Data:     schema.Data,
			}
			mi.schemas[skey] = schema
			mi.schemaHashes[schemaHash] = skey
			mi.nextSchemaID++
		}
	}
	ckey := numkey{idx, channel.ID}
	newChannel, ok := mi.channels[ckey]
	if !ok {
		channelHash := hashChannel(channel)
		if mapped, ok := mi.channelHashes[channelHash]; ok {
			mi.channels[ckey] = mi.channels[mapped]
			newChannel = mi.channels[mapped]
		} else {
			newChannel = &mcap.Channel{
				ID:              mi.nextChannelID,
				SchemaID:        newSchema.ID,
				Topic:           channel.Topic,
				MessageEncoding: channel.MessageEncoding,
				Metadata:        channel.Metadata,
			}
			mi.channels[ckey] = newChannel
			mi.channelHashes[channelHash] = ckey
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
		var item = util.Item[record, uint64]{
			Value:    record{s, c, m, rec.idx},
			Priority: m.LogTime,
		}
		heap.Push(mi.pq, &item)
	}
	s2, c2, m2 := mi.remap(rec.idx, rec.schema, rec.channel, rec.message)
	return s2, c2, m2, nil
}

func NmergeIterator(iterators ...mcap.MessageIterator) (mcap.MessageIterator, error) {
	pq := util.NewPriorityQueue[record, uint64]()
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
		var item = util.Item[record, uint64]{
			Value:    record{schema, channel, message, i},
			Priority: message.LogTime,
		}
		heap.Push(pq, &item)
	}

	return &mergeIterator{
		pq:            pq,
		iterators:     iterators,
		schemaHashes:  map[uint64]numkey{},
		channelHashes: map[uint64]numkey{},
		channels:      map[numkey]*mcap.Channel{},
		schemas:       map[numkey]*mcap.Schema{},
		nextSchemaID:  1,
		nextChannelID: 0,
	}, nil
}
