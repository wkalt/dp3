package mcap

import "github.com/foxglove/mcap/go/mcap"

type MessageIterator interface {
	Next(buf []byte) (*mcap.Schema, *mcap.Channel, *mcap.Message, error)
}
