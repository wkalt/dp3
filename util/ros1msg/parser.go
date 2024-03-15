package ros1msg

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/wkalt/dp3/util/schema"
)

/*
This file implements a recursive descent parser for ROS1 message bytes, with the
capability to parse or skip fields selectively. Our parser avoids parsing bytes
that it does not want to collect statistics for. Today these are,
* Fixed-length arrays with length > 10
* Variable-length arrays

In the future we will want to support at least variable-length complex arrays,
so the strategy will evolve with time.
*/

////////////////////////////////////////////////////////////////////////////////

// Parser is the interface for parsing ROS1 message bytes. FixedSize returns the
// fixed length of the parser if it has one, else zero. The parse method cases
// FixedSize to skip over sections of bytes where possible. The parse parameter
// supplied to it, indicates whether bytes should be parsed or skipped.
type Parser interface {
	FixedSize() int
	Parse(data []byte, values *[]any, parse bool) (int, error)
}

type boolSkipper struct{}

func (s *boolSkipper) FixedSize() int {
	return 1
}

func (s *boolSkipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 1 {
		return 0, ShortReadError{"bool"}
	}
	if parse {
		*values = append(*values, data[0] != 0)
	}
	return 1, nil
}

type uint8Skipper struct{}

func (s *uint8Skipper) FixedSize() int {
	return 1
}

func (s *uint8Skipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 1 {
		return 0, ShortReadError{"uint8"}
	}
	if parse {
		*values = append(*values, data[0])
	}
	return 1, nil
}

type int8Skipper struct{}

func (s *int8Skipper) FixedSize() int {
	return 1
}

func (s *int8Skipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 1 {
		return 0, ShortReadError{"int8"}
	}
	if parse {
		*values = append(*values, int8(data[0]))
	}
	return 1, nil
}

type uint16Skipper struct{}

func (s *uint16Skipper) FixedSize() int {
	return 2
}

func (s *uint16Skipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 2 {
		return 0, ShortReadError{"uint16"}
	}
	if parse {
		*values = append(*values, binary.LittleEndian.Uint16(data))
	}
	return 2, nil
}

type int16Skipper struct{}

func (s *int16Skipper) FixedSize() int {
	return 2
}

func (s *int16Skipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 2 {
		return 0, ShortReadError{"int16"}
	}
	if parse {
		*values = append(*values, int16(binary.LittleEndian.Uint16(data)))
	}
	return 2, nil
}

type uint32Skipper struct{}

func (s *uint32Skipper) FixedSize() int {
	return 4
}

func (s *uint32Skipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 4 {
		return 0, ShortReadError{"uint32"}
	}
	if parse {
		*values = append(*values, binary.LittleEndian.Uint32(data))
	}
	return 4, nil
}

type int32Skipper struct{}

func (s *int32Skipper) FixedSize() int {
	return 4
}

func (s *int32Skipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 4 {
		return 0, ShortReadError{"int32"}
	}
	if parse {
		*values = append(*values, int32(binary.LittleEndian.Uint32(data)))
	}
	return 4, nil
}

type uint64Skipper struct{}

func (s *uint64Skipper) FixedSize() int {
	return 8
}

func (s *uint64Skipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 8 {
		return 0, ShortReadError{"uint64"}
	}
	if parse {
		*values = append(*values, binary.LittleEndian.Uint64(data))
	}
	return 8, nil
}

type int64Skipper struct{}

func (s *int64Skipper) FixedSize() int {
	return 8
}

func (s *int64Skipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 8 {
		return 0, ShortReadError{"int64"}
	}
	if parse {
		*values = append(*values, int64(binary.LittleEndian.Uint64(data)))
	}
	return 8, nil
}

type float32Skipper struct{}

func (s *float32Skipper) FixedSize() int {
	return 4
}

func (s *float32Skipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 4 {
		return 0, ShortReadError{"float32"}
	}
	if parse {
		*values = append(*values, math.Float32frombits(binary.LittleEndian.Uint32(data)))
	}
	return 4, nil
}

type float64Skipper struct{}

func (s *float64Skipper) FixedSize() int {
	return 8
}

func (s *float64Skipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 8 {
		return 0, ShortReadError{"float64"}
	}
	if parse {
		*values = append(*values, math.Float64frombits(binary.LittleEndian.Uint64(data)))
	}
	return 8, nil
}

type stringSkipper struct{}

func (s *stringSkipper) FixedSize() int {
	return 0
}

func (s *stringSkipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 4 {
		return 0, ShortReadError{"string"}
	}
	l := int(binary.LittleEndian.Uint32(data))
	if len(data) < l+4 {
		return 0, ShortReadError{"string"}
	}
	if parse {
		*values = append(*values, string(data[4:4+l]))
	}
	return l + 4, nil
}

type timeSkipper struct{}

func (s *timeSkipper) FixedSize() int {
	return 8
}

func (s *timeSkipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 8 {
		return 0, ShortReadError{"time"}
	}
	secs := binary.LittleEndian.Uint32(data)
	nanos := binary.LittleEndian.Uint32(data[4:])
	if parse {
		*values = append(*values, 1e9*uint64(secs)+uint64(nanos))
	}
	return 8, nil
}

type durationSkipper struct{}

func (s *durationSkipper) FixedSize() int {
	return 8
}

func (s *durationSkipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 8 {
		return 0, ShortReadError{"duration"}
	}
	secs := binary.LittleEndian.Uint32(data)
	nanos := binary.LittleEndian.Uint32(data[4:])
	if parse {
		*values = append(*values, 1e9*int64(secs)+int64(nanos))
	}
	return 8, nil
}

type charSkipper struct{}

func (s *charSkipper) FixedSize() int {
	return 1
}

func (s *charSkipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 1 {
		return 0, ShortReadError{"char"}
	}
	if parse {
		*values = append(*values, rune(data[0]))
	}
	return 1, nil
}

type byteSkipper struct{}

func (s *byteSkipper) FixedSize() int {
	return 1
}

func (s *byteSkipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 1 {
		return 0, ShortReadError{"byte"}
	}
	if parse {
		*values = append(*values, data[0])
	}
	return 1, nil
}

type arraySkipper struct {
	fixedSize int
	items     Parser
}

func (a *arraySkipper) FixedSize() int {
	return a.fixedSize * a.items.FixedSize()
}

func (a *arraySkipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	offset := 0

	if a.fixedSize == 0 { // do not parse fixed-length arrays
		if len(data) < 4 {
			return 0, ShortReadError{"varlen array length"}
		}
		length := int(binary.LittleEndian.Uint32(data))
		offset += 4
		if itemSize := a.items.FixedSize(); itemSize > 0 {
			offset += itemSize * length
			return offset, nil
		}
		for i := 0; i < length; i++ {
			n, err := a.items.Parse(data[offset:], values, false)
			if err != nil {
				return 0, fmt.Errorf("failed to parse array item: %w", err)
			}
			offset += n
		}
		return offset, nil
	}

	// Skip fixed-length arrays with more than 10 elements
	if a.fixedSize > 10 {
		if itemsize := a.items.FixedSize(); itemsize > 0 {
			offset += itemsize * a.fixedSize
			return offset, nil
		}

		for i := 0; i < a.fixedSize; i++ {
			n, err := a.items.Parse(data[offset:], values, false)
			if err != nil {
				return 0, fmt.Errorf("failed to skip array item: %w", err)
			}
			offset += n
		}

		return offset, nil
	}

	// Otherwise, if not parsing try and multiply
	if itemsize := a.items.FixedSize(); itemsize > 0 && !parse {
		offset += itemsize * a.fixedSize
		return offset, nil
	}

	// Otherwise we need to parse
	for i := 0; i < a.fixedSize; i++ {
		n, err := a.items.Parse(data[offset:], values, parse)
		if err != nil {
			return 0, fmt.Errorf("failed to parse array item: %w", err)
		}
		offset += n
	}
	return offset, nil
}

type recordSkipper struct {
	fields []Parser
}

func (r *recordSkipper) FixedSize() int {
	size := 0
	for _, f := range r.fields {
		if fs := f.FixedSize(); fs == 0 {
			return 0
		} else {
			size += fs
		}
	}
	return size
}

func (r *recordSkipper) Parse(data []byte, values *[]any, parse bool) (int, error) {
	offset := 0
	for _, f := range r.fields {
		n, err := f.Parse(data[offset:], values, parse)
		if err != nil {
			return 0, fmt.Errorf("failed to parse field: %w", err)
		}
		offset += n
	}
	return offset, nil
}

func primitiveToFieldSkipper(p schema.PrimitiveType) Parser {
	switch p {
	case schema.INT8:
		return &int8Skipper{}
	case schema.UINT8:
		return &uint8Skipper{}
	case schema.INT16:
		return &int16Skipper{}
	case schema.UINT16:
		return &uint16Skipper{}
	case schema.INT32:
		return &int32Skipper{}
	case schema.UINT32:
		return &uint32Skipper{}
	case schema.INT64:
		return &int64Skipper{}
	case schema.UINT64:
		return &uint64Skipper{}
	case schema.FLOAT32:
		return &float32Skipper{}
	case schema.FLOAT64:
		return &float64Skipper{}
	case schema.STRING:
		return &stringSkipper{}
	case schema.TIME:
		return &timeSkipper{}
	case schema.DURATION:
		return &durationSkipper{}
	case schema.CHAR:
		return &charSkipper{}
	case schema.BYTE:
		return &byteSkipper{}
	case schema.BOOL:
		return &boolSkipper{}
	default:
		panic(fmt.Sprintf("unknown primitive type: %s", p))
	}
}

func arrayToFieldSkipper(a schema.Type) Parser {
	if a.Items.IsPrimitive() {
		return &arraySkipper{
			fixedSize: a.FixedSize,
			items:     primitiveToFieldSkipper(a.Items.Primitive),
		}
	}
	return &arraySkipper{
		fixedSize: a.FixedSize,
		items:     recordToFieldSkipper(a.Items.Fields),
	}
}

func recordToFieldSkipper(fields []schema.Field) Parser {
	skippers := []Parser{}
	for _, field := range fields {
		if field.Type.IsPrimitive() {
			skippers = append(skippers, primitiveToFieldSkipper(field.Type.Primitive))
			continue
		}
		if field.Type.Array {
			skippers = append(skippers, arrayToFieldSkipper(field.Type))
			continue
		}
		if field.Type.Record {
			skippers = append(skippers, recordToFieldSkipper(field.Type.Fields))
			continue
		}
	}
	return &recordSkipper{fields: skippers}
}
