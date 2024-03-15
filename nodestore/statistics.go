package nodestore

import (
	"fmt"

	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/schema"
)

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

// NumericalSummary is a statistical summary of a field.
type NumericalSummary struct {
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
	Sum  float64 `json:"sum"`
}

// TextSummary is a statistical summary of a text field.
type TextSummary struct {
	Min string `json:"min"`
	Max string `json:"max"`
	// todo: bloom filters, trigrams, etc.
}

// Statistics represents the statistics we store on each child element of an inner node.
type Statistics struct {
	Fields       []util.Named[schema.PrimitiveType] `json:"fields"`
	NumStats     map[int]*NumericalSummary          `json:"numeric"`
	TextStats    map[int]*TextSummary               `json:"text"`
	MessageCount uint64                             `json:"messageCount"`
	ByteCount    uint64                             `json:"byteCount"`
}

func (s *Statistics) ObserveNumeric(idx int, v float64) {
	summary, ok := s.NumStats[idx]
	if !ok {
		summary = &NumericalSummary{Min: v, Max: v, Mean: v, Sum: v}
		s.NumStats[idx] = summary
	} else {
		if v < summary.Min {
			summary.Min = v
		}
		if v > summary.Max {
			summary.Max = v
		}
		summary.Sum += v
		summary.Mean = summary.Sum / float64(s.MessageCount+1)
	}
}

func (s *Statistics) ObserveText(idx int, v string) {
	summary, ok := s.TextStats[idx]
	if !ok {
		summary = &TextSummary{Min: v, Max: v}
		s.TextStats[idx] = summary
	} else {
		if v < summary.Min {
			summary.Min = v
		}
		if v > summary.Max {
			summary.Max = v
		}
	}
}

func NewStatistics(fields []util.Named[schema.PrimitiveType]) *Statistics {
	return &Statistics{
		Fields:       fields,
		NumStats:     make(map[int]*NumericalSummary),
		TextStats:    make(map[int]*TextSummary),
		MessageCount: 0,
	}
}

// Add adds the message count from another statistics object to this one.
func (s *Statistics) Add(other *Statistics) error {
	if s.MessageCount == 0 {
		if other != nil {
			*s = *other
		}
		return nil
	}
	s.MessageCount += other.MessageCount
	if len(s.Fields) != len(other.Fields) {
		return fmt.Errorf("mismatched field count: %d != %d", len(s.Fields), len(other.Fields))
	}
	for i := range s.Fields {
		if s.Fields[i] != other.Fields[i] {
			return fmt.Errorf("mismatched field: %v != %v", s.Fields[i], other.Fields[i])
		}
	}
	for i := range s.Fields {
		if numstat, ok := s.NumStats[i]; ok {
			numstat.Min = min(numstat.Min, other.NumStats[i].Min)
			numstat.Max = max(numstat.Max, other.NumStats[i].Max)
			numstat.Sum += other.NumStats[i].Sum
			numstat.Mean = numstat.Sum / float64(s.MessageCount)
		}
		if textstat, ok := s.TextStats[i]; ok {
			textstat.Max = max(textstat.Max, other.TextStats[i].Max)
			textstat.Min = min(textstat.Min, other.TextStats[i].Min)
		}
	}
	return nil
}

// String returns a string representation of the statistics.
func (s *Statistics) String() string {
	return fmt.Sprintf("(count=%d)", s.MessageCount)
}
