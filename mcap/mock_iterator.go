package mcap

import (
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

type mockIterator struct {
	schema  *mcap.Schema
	channel *mcap.Channel
	times   [][]int
}

func (mi *mockIterator) Next([]byte) (
	*mcap.Schema,
	*mcap.Channel,
	*mcap.Message,
	error,
) {
	if len(mi.times) == 0 {
		return nil, nil, nil, io.EOF
	}
	pair := mi.times[0]
	mi.times = mi.times[1:]
	msg := &mcap.Message{
		ChannelID: 1,
		Sequence:  uint32(pair[1]),
		LogTime:   uint64(pair[0]),
	}
	return mi.schema, mi.channel, msg, nil
}

func NewMockIterator(topic string, times [][]int) MessageIterator {
	return &mockIterator{
		schema:  NewSchema(1, topic+"_schema", "", nil),
		channel: NewChannel(0, 1, topic, "", nil),
		times:   times,
	}
}
