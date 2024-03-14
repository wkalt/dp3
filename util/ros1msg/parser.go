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

// Parsers consume a field from a byte slice, returning the number of bytes
// covered by the field. If parse is true, the field is parsed and its value is
// appended to the values array. Otherwise, the field is skipped in place (to
// whatever extent is possible - some lengths may need reading).
type Parser func(data []byte, values *[]any, parse bool) (int, error)

// primitiveToFieldParser returns a FieldParser for a primitive type.
func primitiveToFieldParser(p schema.PrimitiveType) Parser {
	switch p {
	case schema.INT8:
		return parseInt8
	case schema.UINT8:
		return parseUint8
	case schema.INT16:
		return parseInt16
	case schema.UINT16:
		return parseUint16
	case schema.INT32:
		return parseInt32
	case schema.UINT32:
		return parseUint32
	case schema.INT64:
		return parseInt64
	case schema.UINT64:
		return parseUint64
	case schema.FLOAT32:
		return parseFloat32
	case schema.FLOAT64:
		return parseFloat64
	case schema.STRING:
		return parseString
	case schema.TIME:
		return parseTime
	case schema.DURATION:
		return parseDuration
	case schema.CHAR:
		return parseChar
	case schema.BYTE:
		return parseByte
	case schema.BOOL:
		return parseBool
	default:
		panic(fmt.Sprintf("unknown primitive type: %s", p))
	}
}

// arrayToFieldParser returns a FieldParser for an array type.
func arrayToFieldParser(a schema.Type) Parser {
	if a.Items.IsPrimitive() {
		return (&array{
			fixedSize: a.FixedSize,
			items:     primitiveToFieldParser(a.Items.Primitive),
		}).parseArray
	}
	return (&array{
		fixedSize: a.FixedSize,
		items:     recordToFieldParser(a.Items.Fields),
	}).parseArray
}

type record struct {
	fields []Parser
}

func (r *record) parseRecord(data []byte, values *[]any, parse bool) (int, error) {
	offset := 0
	for _, f := range r.fields {
		n, err := f(data[offset:], values, parse)
		if err != nil {
			return 0, fmt.Errorf("failed to parse field: %w", err)
		}
		offset += n
	}
	return offset, nil
}

type array struct {
	fixedSize int
	items     Parser
}

func (a *array) parseArray(data []byte, values *[]any, parse bool) (int, error) {
	offset := 0

	if a.fixedSize == 0 { // do not parse fixed-length arrays
		if len(data) < 4 {
			return 0, ShortReadError{"varlen array length"}
		}
		length := int(binary.LittleEndian.Uint32(data))
		offset += 4
		for i := 0; i < length; i++ {
			n, err := a.items(data[offset:], values, false)
			if err != nil {
				return 0, fmt.Errorf("failed to parse array item: %w", err)
			}
			offset += n
		}
		return offset, nil
	}

	// Skip fixed-length arrays with more than 10 elements
	if a.fixedSize > 10 {
		for i := 0; i < a.fixedSize; i++ {
			n, err := a.items(data[offset:], values, false)
			if err != nil {
				return 0, fmt.Errorf("failed to skip array item: %w", err)
			}
			offset += n
		}
		return offset, nil
	}

	// Otherwise emit if requested
	for i := 0; i < a.fixedSize; i++ {
		n, err := a.items(data[offset:], values, parse)
		if err != nil {
			return 0, fmt.Errorf("failed to parse array item: %w", err)
		}
		offset += n
	}
	return offset, nil
}

// recordToFieldParser returns a FieldParser for a record type.
func recordToFieldParser(fields []schema.Field) Parser {
	parsers := []Parser{}
	for _, field := range fields {
		if field.Type.IsPrimitive() {
			parsers = append(parsers, primitiveToFieldParser(field.Type.Primitive))
			continue
		}
		if field.Type.Array {
			parsers = append(parsers, arrayToFieldParser(field.Type))
			continue
		}

		if field.Type.Record {
			parsers = append(parsers, recordToFieldParser(field.Type.Fields))
			continue
		}
	}
	return (&record{fields: parsers}).parseRecord
}

func parseBool(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 1 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, data[0] != 0)
	}
	return 1, nil
}

func parseInt8(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 1 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, int8(data[0]))
	}
	return 1, nil
}

func parseUint8(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 1 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, data[0])
	}
	return 1, nil
}

func parseInt16(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 2 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, int16(binary.LittleEndian.Uint16(data)))
	}
	return 2, nil
}

func parseUint16(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 2 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, binary.LittleEndian.Uint16(data))
	}
	return 2, nil
}

func parseInt32(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 4 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, int32(binary.LittleEndian.Uint32(data)))
	}
	return 4, nil
}

func parseUint32(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 4 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, binary.LittleEndian.Uint32(data))
	}
	return 4, nil
}

func parseInt64(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 8 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, int64(binary.LittleEndian.Uint64(data)))
	}
	return 8, nil
}

func parseUint64(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 8 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, binary.LittleEndian.Uint64(data))
	}
	return 8, nil
}

func parseFloat32(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 4 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, math.Float32frombits(binary.LittleEndian.Uint32(data)))
	}
	return 4, nil
}

func parseFloat64(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 8 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, math.Float64frombits(binary.LittleEndian.Uint64(data)))
	}
	return 8, nil
}

func parseString(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 4 {
		return 0, ErrShortRead
	}
	l := int(binary.LittleEndian.Uint32(data))
	if len(data) < l+4 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, string(data[4:4+l]))
	}
	return l + 4, nil
}

func parseTime(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 8 {
		return 0, ErrShortRead
	}
	secs := binary.LittleEndian.Uint32(data)
	nanos := binary.LittleEndian.Uint32(data[4:])
	if parse {
		*values = append(*values, 1e9*uint64(secs)+uint64(nanos))
	}
	return 8, nil
}

func parseDuration(data []byte, values *[]any, parse bool) (int, error) {
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

func parseChar(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 1 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, rune(data[0]))
	}
	return 1, nil
}

func parseByte(data []byte, values *[]any, parse bool) (int, error) {
	if len(data) < 1 {
		return 0, ErrShortRead
	}
	if parse {
		*values = append(*values, data[0])
	}
	return 1, nil
}
