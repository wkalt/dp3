package nodestore

import (
	"fmt"
	"math"

	fmcap "github.com/foxglove/mcap/go/mcap"
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

type StatType string

const (
	Float StatType = "float"
	Int   StatType = "int"
	Text  StatType = "text"
)

// NumericalSummary is a statistical summary of a field.
type NumericalSummary struct {
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
	Sum  float64 `json:"sum"`
}

func (n *NumericalSummary) ranges(field string, start, end uint64) []StatRange {
	return []StatRange{
		{
			Start: start,
			End:   end,
			Type:  Float,
			Name:  "mean",
			Field: field,
			Value: n.Mean,
		},
		{
			Start: start,
			End:   end,
			Type:  Float,
			Name:  "min",
			Field: field,
			Value: n.Min,
		},
		{
			Start: start,
			End:   end,
			Type:  Float,
			Name:  "max",
			Field: field,
			Value: n.Max,
		},
		{
			Start: start,
			End:   end,
			Type:  Float,
			Name:  "sum",
			Field: field,
			Value: n.Sum,
		},
	}
}

// TextSummary is a statistical summary of a text field.
type TextSummary struct {
	Min string `json:"min"`
	Max string `json:"max"`
	// todo: bloom filters, trigrams, etc.
}

func (s *TextSummary) ranges(field string, start, end uint64) []StatRange {
	return []StatRange{
		{
			Start: start,
			End:   end,
			Type:  Text,
			Name:  "min",
			Field: field,
			Value: s.Min,
		},
		{
			Start: start,
			End:   end,
			Type:  Text,
			Name:  "max",
			Field: field,
			Value: s.Max,
		},
	}
}

// StatRange is a range of statistics.
type StatRange struct {
	Start uint64   `json:"start"`
	End   uint64   `json:"end"`
	Type  StatType `json:"type"`
	Field string   `json:"field"`
	Name  string   `json:"name"`
	Value any      `json:"value"`
}

func NewStatRange(
	start, end uint64,
	typ StatType,
	field, name string,
	value any,
) StatRange {
	return StatRange{
		Start: start,
		End:   end,
		Type:  typ,
		Field: field,
		Name:  name,
		Value: value,
	}
}

// Statistics represents the statistics we store on each child element of an inner node.
type Statistics struct {
	Fields          []util.Named[schema.PrimitiveType] `json:"fields,omitempty"`
	NumStats        map[int]*NumericalSummary          `json:"numeric,omitempty"`
	TextStats       map[int]*TextSummary               `json:"text,omitempty"`
	MessageCount    int64                              `json:"messageCount"`
	ByteCount       int64                              `json:"byteCount"`
	MaxObservedTime int64                              `json:"maxObservedTime"`
	MinObservedTime int64                              `json:"minObservedTime"`
}

// Ranges converts a statistics object into an array of StatRange objects,
// suitable for returning to a user.
func (s *Statistics) Ranges(start, end uint64) []StatRange {
	ranges := make([]StatRange, 0, len(s.NumStats)+len(s.TextStats))
	for i, field := range s.Fields {
		if numstat, ok := s.NumStats[i]; ok {
			ranges = append(ranges, numstat.ranges(field.Name, start, end)...)
			continue
		}
		if textstat, ok := s.TextStats[i]; ok {
			ranges = append(ranges, textstat.ranges(field.Name, start, end)...)
		}
	}
	ranges = append(ranges, []StatRange{
		{
			Start: start,
			End:   end,
			Type:  Int,
			Field: "",
			Name:  "messageCount",
			Value: s.MessageCount,
		},
		{
			Start: start,
			End:   end,
			Type:  Int,
			Field: "",
			Name:  "byteCount",
			Value: s.ByteCount,
		},
		{
			Start: start,
			End:   end,
			Type:  Int,
			Field: "",
			Name:  "minObservedTime",
			Value: s.MinObservedTime,
		},
		{
			Start: start,
			End:   end,
			Type:  Int,
			Field: "",
			Name:  "maxObservedTime",
			Value: s.MaxObservedTime,
		},
	}...)
	return ranges
}

func (s *Statistics) observeNumeric(idx int, v float64) {
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

func (s *Statistics) observeText(idx int, v string) {
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

func toFloat(x any) (float64, error) {
	switch x := x.(type) {
	case int8:
		return float64(x), nil
	case int16:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	case uint8:
		return float64(x), nil
	case uint16:
		return float64(x), nil
	case uint32:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	default:
		return 0, fmt.Errorf("unsupported type: %T", x)
	}
}

func (s *Statistics) ObserveMessage(message *fmcap.Message, values []any) error {
	if len(values) != len(s.Fields) {
		return fmt.Errorf("mismatched field count: %d != %d", len(values), len(s.Fields))
	}
	for i, field := range s.Fields {
		value := values[i]
		switch field.Value {
		case schema.INT8, schema.INT16, schema.INT32, schema.INT64, schema.FLOAT32, schema.FLOAT64,
			schema.UINT8, schema.UINT16, schema.UINT32, schema.UINT64:
			v, err := toFloat(value)
			if err != nil {
				return fmt.Errorf("failed to convert value to float: %w", err)
			}
			s.observeNumeric(i, v)
		case schema.STRING:
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("expected string, got %T", value)
			}
			s.observeText(i, v)
		}
	}
	s.MessageCount++
	s.ByteCount += int64(len(message.Data))
	if message.LogTime < uint64(s.MinObservedTime) {
		s.MinObservedTime = int64(message.LogTime)
	}
	if message.LogTime > uint64(s.MaxObservedTime) {
		s.MaxObservedTime = int64(message.LogTime)
	}
	return nil
}

func NewStatistics(fields []util.Named[schema.PrimitiveType]) *Statistics {
	return &Statistics{
		Fields:          fields,
		NumStats:        make(map[int]*NumericalSummary),
		TextStats:       make(map[int]*TextSummary),
		MessageCount:    0,
		MinObservedTime: math.MaxInt64,
		MaxObservedTime: 0,
	}
}

// Add adds the statistics from another Statistics object to this one.
func (s *Statistics) Add(other *Statistics) error {
	if s.MessageCount == 0 {
		if other != nil {
			*s = *other
		}
		return nil
	}
	s.MessageCount += other.MessageCount
	s.ByteCount += other.ByteCount
	s.MinObservedTime = min(s.MinObservedTime, other.MinObservedTime)
	s.MaxObservedTime = max(s.MaxObservedTime, other.MaxObservedTime)
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
