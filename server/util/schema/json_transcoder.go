package schema

import (
	"encoding/base64"
	"fmt"
	"io"
	"strconv"

	"github.com/wkalt/dp3/server/util"
)

type JSONTranscoder struct {
	p      *Parser
	schema *Schema

	buf []byte

	stack []element
}

type element struct {
	opener    string
	fieldname string
	typ       Type
	closer    string
}

func NewJSONTranscoder(schema *Schema, decoder Decoder) (*JSONTranscoder, error) {
	parser, err := NewParser(schema, nil, decoder)
	if err != nil {
		return nil, err
	}
	return &JSONTranscoder{
		p:      parser,
		schema: schema,
		buf:    []byte{},
	}, nil
}

func (t *JSONTranscoder) formatPrimitive(typ PrimitiveType, value any) error {
	switch typ {
	case FLOAT64:
		return t.formatFloat64(value)
	case FLOAT32:
		return t.formatFloat32(value)
	case BOOL:
		return t.formatBool(value)
	case INT8:
		return t.formatInt8(value)
	case INT16:
		return t.formatInt16(value)
	case INT32:
		return t.formatInt32(value)
	case INT64:
		return t.formatInt64(value)
	case UINT8:
		return t.formatUint8(value)
	case UINT16:
		return t.formatUint16(value)
	case UINT32:
		return t.formatUint32(value)
	case UINT64:
		return t.formatUint64(value)
	case STRING:
		return t.formatString(value)
	case TIME:
		return t.formatTime(value)
	case DURATION:
		return t.formatDuration(value)
	case CHAR:
		return t.formatChar(value)
	case BYTE:
		return t.formatByte(value)
	default:
		return fmt.Errorf("invalid type %v", typ)
	}
}

func (t *JSONTranscoder) formatFloat64(value any) error {
	v, ok := value.(float64)
	if !ok {
		return fmt.Errorf("invalid type %T for FLOAT64", value)
	}
	t.buf = strconv.AppendFloat(t.buf, v, 'f', -1, 64)
	return nil
}

func (t *JSONTranscoder) formatFloat32(value any) error {
	v, ok := value.(float32)
	if !ok {
		return fmt.Errorf("invalid type %T for FLOAT32", value)
	}
	t.buf = strconv.AppendFloat(t.buf, float64(v), 'f', -1, 32)
	return nil
}

func (t *JSONTranscoder) formatBool(value any) error {
	v, ok := value.(bool)
	if !ok {
		return fmt.Errorf("invalid type %T for BOOL", value)
	}
	t.buf = strconv.AppendBool(t.buf, v)
	return nil
}

func (t *JSONTranscoder) formatInt8(value any) error {
	v, ok := value.(int8)
	if !ok {
		return fmt.Errorf("invalid type %T for INT8", value)
	}
	t.buf = strconv.AppendInt(t.buf, int64(v), 10)
	return nil
}

func (t *JSONTranscoder) formatInt16(value any) error {
	v, ok := value.(int16)
	if !ok {
		return fmt.Errorf("invalid type %T for INT16", value)
	}
	t.buf = strconv.AppendInt(t.buf, int64(v), 10)
	return nil
}

func (t *JSONTranscoder) formatInt32(value any) error {
	v, ok := value.(int32)
	if !ok {
		return fmt.Errorf("invalid type %T for INT32", value)
	}
	t.buf = strconv.AppendInt(t.buf, int64(v), 10)
	return nil
}

func (t *JSONTranscoder) formatInt64(value any) error {
	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("invalid type %T for INT64", value)
	}
	t.buf = strconv.AppendInt(t.buf, v, 10)
	return nil
}

func (t *JSONTranscoder) formatUint8(value any) error {
	v, ok := value.(uint8)
	if !ok {
		return fmt.Errorf("invalid type %T for UINT8", value)
	}
	t.buf = strconv.AppendUint(t.buf, uint64(v), 10)
	return nil
}

func (t *JSONTranscoder) formatUint16(value any) error {
	v, ok := value.(uint16)
	if !ok {
		return fmt.Errorf("invalid type %T for UINT16", value)
	}
	t.buf = strconv.AppendUint(t.buf, uint64(v), 10)
	return nil
}

func (t *JSONTranscoder) formatUint32(value any) error {
	v, ok := value.(uint32)
	if !ok {
		return fmt.Errorf("invalid type %T for UINT32", value)
	}
	t.buf = strconv.AppendUint(t.buf, uint64(v), 10)
	return nil
}

func (t *JSONTranscoder) formatUint64(value any) error {
	v, ok := value.(uint64)
	if !ok {
		return fmt.Errorf("invalid type %T for UINT64", value)
	}
	t.buf = strconv.AppendUint(t.buf, v, 10)
	return nil
}

func (t *JSONTranscoder) formatString(value any) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("invalid type %T for STRING", value)
	}
	t.buf = append(t.buf, '"')
	t.buf = append(t.buf, []byte(v)...)
	t.buf = append(t.buf, '"')
	return nil
}

func (t *JSONTranscoder) formatTime(value any) error {
	v, ok := value.(uint64)
	if !ok {
		return fmt.Errorf("invalid type %T for TIME", value)
	}
	t.buf = strconv.AppendUint(t.buf, v, 10)
	return nil
}

func (t *JSONTranscoder) formatDuration(value any) error {
	v, ok := value.(uint64)
	if !ok {
		return fmt.Errorf("invalid type %T for DURATION", value)
	}
	t.buf = strconv.AppendUint(t.buf, v, 10)
	return nil
}

func (t *JSONTranscoder) formatChar(value any) error {
	v, ok := value.(uint8)
	if !ok {
		return fmt.Errorf("invalid type %T for CHAR", value)
	}
	t.buf = strconv.AppendUint(t.buf, uint64(v), 10)
	return nil
}

func (t *JSONTranscoder) formatByte(value any) error {
	v, ok := value.(uint8)
	if !ok {
		return fmt.Errorf("invalid type %T for BYTE", value)
	}
	t.buf = strconv.AppendUint(t.buf, uint64(v), 10)
	return nil
}

func (t *JSONTranscoder) Transcode(w io.Writer, buf []byte) error { // nolint: funlen
	t.buf = t.buf[:0]
	breaks, values, err := t.p.Parse(buf)
	if err != nil {
		return err
	}
	for i, field := range t.schema.Fields {
		var opener, closer string
		if i == 0 {
			opener = "{"
		}
		if i == len(t.schema.Fields)-1 {
			closer = "}"
		}
		t.stack = t.stack[:0]
		t.stack = append(t.stack, element{opener, field.Name, field.Type, closer})
		for len(t.stack) > 0 {
			e := t.stack[len(t.stack)-1]
			t.stack = t.stack[:len(t.stack)-1]
			if e.opener != "" {
				t.buf = append(t.buf, e.opener...)
			} else {
				t.buf = append(t.buf, ',')
			}
			if e.fieldname != "" {
				t.buf = append(t.buf, '"')
				t.buf = append(t.buf, []byte(e.fieldname)...)
				t.buf = append(t.buf, `":`...)
			}
			if e.typ.IsPrimitive() {
				value := values[0]
				values = values[1:]
				if err := t.formatPrimitive(e.typ.Primitive, value); err != nil {
					return fmt.Errorf("failed to format primitive: %w", err)
				}
				if e.closer != "" {
					t.buf = append(t.buf, e.closer...)
				}
				continue
			}

			if e.typ.Array { // nolint: nestif
				length := breaks[0]
				breaks = breaks[1:]
				items := e.typ.Items
				// if it's a byte array, format it with base64 and bypass the stack.
				// apply optimizations for fixed-width types
				if items.IsPrimitive() {
					switch items.Primitive {
					case UINT8, BYTE:
						value, ok := values[0].([]byte)
						if !ok {
							return fmt.Errorf("invalid type %T for BYTE", values[0])
						}
						values = values[1:]

						t.buf = append(t.buf, '"')
						t.buf = base64.StdEncoding.AppendEncode(t.buf, value)
						t.buf = append(t.buf, '"')
						if e.closer != "" {
							t.buf = append(t.buf, e.closer...)
						}
						continue
					}
				}

				if length == 0 {
					t.buf = append(t.buf, "[]"+e.closer...)
					continue
				}

				for j := length - 1; j >= 0; j-- {
					opener := util.When(j == 0, "[", "")
					closer := util.When(j == length-1, "]"+e.closer, "")
					t.stack = append(t.stack, element{opener, "", *items, closer})
				}
				continue
			}
			if e.typ.Record {
				fields := e.typ.Fields
				length := len(fields)
				if length == 0 {
					t.buf = append(t.buf, "{}"+e.closer...)
					continue
				}
				for j := length - 1; j >= 0; j-- {
					opener := util.When(j == 0, "{", "")
					closer := util.When(j == length-1, "}"+e.closer, "")
					t.stack = append(t.stack, element{opener, fields[j].Name, fields[j].Type, closer})
				}
			}
		}
	}
	if _, err = w.Write(t.buf); err != nil {
		return fmt.Errorf("failed to write transcoded buffer: %w", err)
	}
	return nil
}
