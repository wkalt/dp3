package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/wkalt/dp3/server/util"
)

type NodeStats struct {
	bytesOut  int
	tuplesOut int

	startTime           time.Time
	elapsedToFirstTuple time.Duration
	elapsedToLastTuple  time.Duration

	initialized bool

	child Node

	label string

	lastTupleRecorded bool
}

func NewNodeStats(child Node, label string) *NodeStats {
	return &NodeStats{
		child: child,
		label: label,
	}
}

func (n *NodeStats) Next(ctx context.Context) (*Tuple, error) {
	if !n.initialized {
		n.StartTimer()
		n.initialized = true
	}
	tup, err := n.child.Next(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			n.RecordLastTuple()
		}
		return tup, fmt.Errorf("failed to get next record: %w", err)
	}
	n.IncTuplesOut(1)
	n.IncBytesOut(len(tup.message.Data))
	return tup, nil
}

func (n *NodeStats) String() string {
	return n.child.String()
}

func (n *NodeStats) Close(ctx context.Context) error {
	if !n.lastTupleRecorded {
		n.RecordLastTuple()
	}
	ctx, _ = util.WithChildContext(ctx, n.label)
	util.SetContextValue(ctx, "bytes_out", float64(n.bytesOut))
	util.SetContextValue(ctx, "tuples_out", float64(n.tuplesOut))
	util.SetContextValue(
		ctx, "elapsed_to_first_tuple", float64(n.elapsedToFirstTuple.Milliseconds()))
	util.SetContextValue(
		ctx, "elapsed_to_last_tuple", float64(n.elapsedToLastTuple.Milliseconds()))
	if err := n.child.Close(ctx); err != nil {
		return fmt.Errorf("failed to close child: %w", err)
	}
	return nil
}

func (n *NodeStats) IncBytesOut(bytes int) {
	n.bytesOut += bytes
}

func (n *NodeStats) IncTuplesOut(tuples int) {
	n.tuplesOut += tuples
}

func (n *NodeStats) StartTimer() {
	n.startTime = time.Now()
}

func (n *NodeStats) RecordFirstTuple() {
	n.elapsedToFirstTuple = time.Since(n.startTime)
}

func (n *NodeStats) RecordLastTuple() {
	n.elapsedToLastTuple = time.Since(n.startTime)
	n.lastTupleRecorded = true
}
