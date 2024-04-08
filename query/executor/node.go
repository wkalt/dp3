package executor

import (
	"context"

	fmcap "github.com/foxglove/mcap/go/mcap"
)

/*
All operators in the execution plan implement the Node interface. A plan is a
tree of operators, and is executed by repeatedly calling Next() on the root,
until an io.EOF is received.

The String() method is used to recursively generate a human-readable
representation of the plan. We use it for tests.
*/

////////////////////////////////////////////////////////////////////////////////

// Tuple represents a tuple of data, consisting of a schema pointer, channel
// pointer, and message pointer. It is likely that we will find a more efficient
// way to represent this in the future, since technically the message includes a
// channel ID, and channel includes a schema ID, so with a mapping we could get
// by with passing just one thing. The current interface results from us leaning
// on the Foxglove MCAP SDK. Once this rises to the top of the priority list we
// can replace it with our own.
type Tuple struct {
	Schema  *fmcap.Schema
	Channel *fmcap.Channel
	Message *fmcap.Message
}

// NewTuple constructs a new tuple.
func NewTuple(schema *fmcap.Schema, channel *fmcap.Channel, message *fmcap.Message) *Tuple {
	return &Tuple{Schema: schema, Channel: channel, Message: message}
}

// Node is the interface for all operators in the execution plan.
type Node interface {
	Next(ctx context.Context) (*Tuple, error)
	String() string
	Close() error
}
