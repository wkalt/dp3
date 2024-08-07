package nodestore

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/store"
	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/util"
	"github.com/wkalt/dp3/server/util/schema"
	"github.com/wkalt/dp3/server/util/trigram"
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

type dsketch struct {
	mtx  *sync.Mutex
	data []byte
	*ddsketch.DDSketch
}

func (d *dsketch) parse() error {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.DDSketch != nil {
		return nil
	}
	data := d.data
	sketch, err := ddsketch.DecodeDDSketch(data, store.BufferedPaginatedStoreConstructor, nil)
	if err != nil {
		return fmt.Errorf("failed to decode ddsketch: %w", err)
	}
	d.DDSketch = sketch
	d.data = nil
	return nil
}

func (d *dsketch) GetSketch() (*ddsketch.DDSketch, error) {
	if d.DDSketch != nil {
		return d.DDSketch, nil
	}
	if err := d.parse(); err != nil {
		return nil, fmt.Errorf("failed to parse ddsketch: %w", err)
	}
	return d.DDSketch, nil
}

func (d dsketch) MarshalJSON() ([]byte, error) {
	buf := []byte{}
	if d.DDSketch == nil {
		if err := d.parse(); err != nil {
			return nil, fmt.Errorf("failed to parse ddsketch: %w", err)
		}
	}

	d.Encode(&buf, false)
	data, err := json.Marshal(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal buffer: %w", err)
	}
	return data, nil
}

func (d *dsketch) UnmarshalJSON(data []byte) error {
	var buf []byte
	if err := json.Unmarshal(data, &buf); err != nil {
		return fmt.Errorf("failed to unmarshal buffer: %w", err)
	}
	d.data = buf
	d.mtx = &sync.Mutex{}
	return nil
}

// NumericalSummary is a statistical summary of a field.
type NumericalSummary struct {
	Min      float64 `json:"min"`
	Max      float64 `json:"max"`
	Mean     float64 `json:"mean"`
	Sum      float64 `json:"sum"`
	Count    float64 `json:"count"`
	DDSketch dsketch `json:"ddsketch"`
}

func (n *NumericalSummary) Observe(v float64) error {
	// skip NaN and inf values. We may need to revisit this with something
	// smarter.
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	if v < n.Min {
		n.Min = v
	}
	if v > n.Max {
		n.Max = v
	}
	n.Count++
	n.Sum += v
	n.Mean = n.Sum / n.Count
	if err := n.DDSketch.Add(v); err != nil {
		return fmt.Errorf("failed to add value to ddsketch: %w", err)
	}
	return nil
}

func NewNumericalSummary() (*NumericalSummary, error) {
	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	if err != nil {
		return nil, fmt.Errorf("failed to create ddsketch: %w", err)
	}
	return &NumericalSummary{
		Min:      math.MaxFloat64,
		Max:      -math.MaxFloat64,
		Mean:     0,
		Sum:      0,
		Count:    0,
		DDSketch: dsketch{&sync.Mutex{}, nil, sketch},
	}, nil
}

func (n *NumericalSummary) Merge(other *NumericalSummary) error {
	n.Min = min(n.Min, other.Min)
	n.Max = max(n.Max, other.Max)
	n.Sum += other.Sum
	n.Count += other.Count
	n.Mean = n.Sum / n.Count

	if n.DDSketch.DDSketch == nil {
		if err := n.DDSketch.parse(); err != nil {
			return fmt.Errorf("failed to parse ddsketch: %w", err)
		}
	}
	if other.DDSketch.DDSketch == nil {
		if err := other.DDSketch.parse(); err != nil {
			return fmt.Errorf("failed to parse ddsketch: %w", err)
		}
	}

	if err := n.DDSketch.MergeWith(other.DDSketch.DDSketch); err != nil {
		return fmt.Errorf("failed to merge ddsketch: %w", err)
	}
	return nil
}

func newStatRange(
	start, end uint64,
	typ StatType,
	field, name, schemaHash string,
	value any,
) StatRange {
	return StatRange{
		Start:      start,
		End:        end,
		Type:       typ,
		Field:      field,
		Name:       name,
		SchemaHash: schemaHash,
		Value:      value,
	}
}

func (n *NumericalSummary) ranges(
	field string,
	start,
	end uint64,
	schemaHash string,
) ([]StatRange, error) {
	qs := []float64{
		0.25, 0.5, 0.75, 0.9, 0.95, 0.99,
	}
	if n.DDSketch.DDSketch == nil {
		if err := n.DDSketch.parse(); err != nil {
			return nil, fmt.Errorf("failed to parse ddsketch: %w", err)
		}
	}
	quantiles, err := n.DDSketch.GetValuesAtQuantiles(qs)
	if err != nil {
		return nil, fmt.Errorf("failed to get quantiles: %w", err)
	}
	return []StatRange{
		newStatRange(start, end, Float, field, "P25", schemaHash, quantiles[0]),
		newStatRange(start, end, Float, field, "P50", schemaHash, quantiles[1]),
		newStatRange(start, end, Float, field, "P75", schemaHash, quantiles[2]),
		newStatRange(start, end, Float, field, "P90", schemaHash, quantiles[3]),
		newStatRange(start, end, Float, field, "P95", schemaHash, quantiles[4]),
		newStatRange(start, end, Float, field, "P99", schemaHash, quantiles[5]),
		newStatRange(start, end, Float, field, "mean", schemaHash, n.Mean),
		newStatRange(start, end, Float, field, "min", schemaHash, n.Min),
		newStatRange(start, end, Float, field, "max", schemaHash, n.Max),
		newStatRange(start, end, Float, field, "sum", schemaHash, n.Sum),
		newStatRange(start, end, Float, field, "count", schemaHash, n.Count),
	}, nil
}

// TextSummary is a statistical summary of a text field.
type TextSummary struct {
	nonempty bool
	Min      string `json:"min"`
	Max      string `json:"max"`

	TrigramSignature trigram.Signature `json:"trgmSignature"`
}

func (s *TextSummary) Merge(other *TextSummary) {
	if !s.nonempty {
		*s = *other
		return
	}
	s.Min = min(s.Min, other.Min)
	s.Max = max(s.Max, other.Max)
	s.TrigramSignature.Add(other.TrigramSignature)
}

func (s *TextSummary) ranges(field string, start, end uint64, schemaHash string) []StatRange {
	return []StatRange{
		{
			Start:      start,
			End:        end,
			Type:       Text,
			SchemaHash: schemaHash,
			Name:       "min",
			Field:      field,
			Value:      s.Min,
		},
		{
			Start:      start,
			End:        end,
			Type:       Text,
			SchemaHash: schemaHash,
			Name:       "max",
			Field:      field,
			Value:      s.Max,
		},
	}
}

// StatRange is a range of statistics.
type StatRange struct {
	Start      uint64   `json:"start"`
	End        uint64   `json:"end"`
	SchemaHash string   `json:"schemaHash"`
	Type       StatType `json:"type"`
	Field      string   `json:"field"`
	Name       string   `json:"name"`
	Value      any      `json:"value"`
}

func NewStatRange(
	schemaHash string,
	start, end uint64,
	typ StatType,
	field, name string,
	value any,
) StatRange {
	return StatRange{
		Start:      start,
		End:        end,
		Type:       typ,
		Field:      field,
		SchemaHash: schemaHash,
		Name:       name,
		Value:      value,
	}
}

// Statistics represents the statistics we store on each child element of an inner node.
type Statistics struct {
	Fields            []util.Named[schema.PrimitiveType] `json:"fields,omitempty"`
	NumStats          map[int]*NumericalSummary          `json:"numeric,omitempty"`
	TextStats         map[int]*TextSummary               `json:"text,omitempty"`
	MessageCount      int64                              `json:"messageCount"`
	BytesUncompressed int64                              `json:"bytesUncompressed"`
	MaxObservedTime   int64                              `json:"maxObservedTime"`
	MinObservedTime   int64                              `json:"minObservedTime"`
}

// Ranges converts a statistics object into an array of StatRange objects,
// suitable for returning to a user.
func (s *Statistics) Ranges(start, end uint64, schemaHash string) ([]StatRange, error) {
	ranges := make([]StatRange, 0, len(s.NumStats)+len(s.TextStats))
	for i, field := range s.Fields {
		if numstat, ok := s.NumStats[i]; ok {
			numranges, err := numstat.ranges(field.Name, start, end, schemaHash)
			if err != nil {
				return nil, err
			}
			ranges = append(ranges, numranges...)
			continue
		}
		if textstat, ok := s.TextStats[i]; ok {
			ranges = append(ranges, textstat.ranges(field.Name, start, end, schemaHash)...)
		}
	}
	ranges = append(ranges, []StatRange{
		newStatRange(start, end, Int, "", "messageCount", schemaHash, s.MessageCount),
		newStatRange(start, end, Int, "", "bytesUncompressed", schemaHash, s.BytesUncompressed),
		newStatRange(start, end, Int, "", "minObservedTime", schemaHash, s.MinObservedTime),
		newStatRange(start, end, Int, "", "maxObservedTime", schemaHash, s.MaxObservedTime),
	}...)
	return ranges, nil
}

func (s *Statistics) observeNumeric(idx int, v float64) (err error) {
	summary, ok := s.NumStats[idx]
	if !ok {
		summary, err = NewNumericalSummary()
		if err != nil {
			return err
		}
		s.NumStats[idx] = summary
	}
	if err := summary.Observe(v); err != nil {
		return err
	}
	return nil
}

func (s *Statistics) observeText(idx int, v string) {
	summary, ok := s.TextStats[idx]
	if !ok {
		summary = &TextSummary{Min: v, Max: v, TrigramSignature: trigram.NewSignature(12)}
		summary.TrigramSignature.AddString(v)
		s.TextStats[idx] = summary
	} else {
		if v < summary.Min {
			summary.Min = v
		}
		if v > summary.Max {
			summary.Max = v
		}
		summary.TrigramSignature.AddString(v)
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
			if err := s.observeNumeric(i, v); err != nil {
				return fmt.Errorf("failed to observe numeric value: %w", err)
			}
		case schema.STRING:
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("expected string, got %T", value)
			}
			s.observeText(i, v)
		}
	}
	s.MessageCount++
	s.BytesUncompressed += int64(len(message.Data))
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

func (s *Statistics) Clone() *Statistics {
	fields := make([]util.Named[schema.PrimitiveType], len(s.Fields))
	copy(fields, s.Fields)
	numStats := make(map[int]*NumericalSummary, len(s.NumStats))
	for i, numStat := range s.NumStats {
		stat := *numStat
		numStats[i] = &stat
	}
	textStats := make(map[int]*TextSummary, len(s.TextStats))
	for i, textStat := range s.TextStats {
		stat := *textStat
		textStats[i] = &stat
	}
	return &Statistics{
		Fields:            fields,
		NumStats:          numStats,
		TextStats:         textStats,
		MessageCount:      s.MessageCount,
		BytesUncompressed: s.BytesUncompressed,
		MinObservedTime:   s.MinObservedTime,
		MaxObservedTime:   s.MaxObservedTime,
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
	s.BytesUncompressed += other.BytesUncompressed
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
			numstat.Count += other.NumStats[i].Count
			if numstat.Count == 0 {
				continue // or NaNs will result. Remove when we support NaN statistics.
			}
			numstat.Min = min(numstat.Min, other.NumStats[i].Min)
			numstat.Max = max(numstat.Max, other.NumStats[i].Max)
			numstat.Sum += other.NumStats[i].Sum
			numstat.Mean = numstat.Sum / numstat.Count
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
	return fmt.Sprintf("count=%d", s.MessageCount)
}

func MergeStatsMaps(a map[string]*Statistics, b map[string]*Statistics) (map[string]*Statistics, error) {
	m := make(map[string]*Statistics)
	for k, v := range b {
		if _, ok := a[k]; ok {
			left := a[k].Clone()
			if err := left.Add(v); err != nil {
				return nil, fmt.Errorf("failed to add statistics: %w", err)
			}
			m[k] = left
		} else {
			m[k] = v.Clone()
		}
	}
	for k, v := range a {
		if _, ok := b[k]; !ok {
			m[k] = v.Clone()
		}
	}
	return m, nil
}

func CloneStatsMap(stats map[string]*Statistics) map[string]*Statistics {
	clone := make(map[string]*Statistics, len(stats))
	for k, v := range stats {
		clone[k] = v.Clone()
	}
	return clone
}
