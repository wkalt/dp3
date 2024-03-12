package nodestore

import "fmt"

/*
Statistics are the statistics we store on each child element of an inner node.
Today this is just a message count but in the future we'll have field-level
stats here.

We are limited to "associative" statistics, meaning statistics that we can
compute from the old statistic + new data. These are the ones we can efficiently
compute on insert without revisiting old leaves. Not all statistics are like
this - for instance quantiles are not. However there is a family of data
structures called sketches that are focused around this kind of issue and should
give us approximate alternatives.
*/

////////////////////////////////////////////////////////////////////////////////

// Statistics represents the statistics we store on each child element of an inner node.
type Statistics struct {
	MessageCount uint64 `json:"messageCount"`
}

// Add adds the message count from another statistics object to this one.
func (s *Statistics) Add(other *Statistics) {
	s.MessageCount += other.MessageCount
}

// String returns a string representation of the statistics.
func (s *Statistics) String() string {
	return fmt.Sprintf("(count=%d)", s.MessageCount)
}
