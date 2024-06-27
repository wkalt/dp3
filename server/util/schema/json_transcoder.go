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

func (t *JSONTranscoder) formatPrimitive(typ PrimitiveType, value any) {
	switch typ {
	case FLOAT64:
		t.buf = strconv.AppendFloat(t.buf, value.(float64), 'f', -1, 64)
	case FLOAT32:
		t.buf = strconv.AppendFloat(t.buf, float64(value.(float32)), 'f', -1, 32)
	case BOOL:
		t.buf = strconv.AppendBool(t.buf, value.(bool))
	case INT8:
		t.buf = strconv.AppendInt(t.buf, int64(value.(int8)), 10)
	case INT16:
		t.buf = strconv.AppendInt(t.buf, int64(value.(int16)), 10)
	case INT32:
		t.buf = strconv.AppendInt(t.buf, int64(value.(int32)), 10)
	case INT64:
		t.buf = strconv.AppendInt(t.buf, value.(int64), 10)
	case UINT8:
		t.buf = strconv.AppendUint(t.buf, uint64(value.(uint8)), 10)
	case UINT16:
		t.buf = strconv.AppendUint(t.buf, uint64(value.(uint16)), 10)
	case UINT32:
		t.buf = strconv.AppendUint(t.buf, uint64(value.(uint32)), 10)
	case UINT64:
		t.buf = strconv.AppendUint(t.buf, value.(uint64), 10)
	case STRING:
		t.buf = append(t.buf, '"')
		t.buf = append(t.buf, []byte(value.(string))...)
		t.buf = append(t.buf, '"')
	case TIME:
		t.buf = strconv.AppendUint(t.buf, value.(uint64), 10)
	case DURATION:
		t.buf = strconv.AppendUint(t.buf, value.(uint64), 10)
	case CHAR:
		t.buf = strconv.AppendUint(t.buf, uint64(value.(uint8)), 10)
	case BYTE:
		t.buf = strconv.AppendUint(t.buf, uint64(value.(uint8)), 10)
	default:
		panic("invalid type")
	}
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
				t.formatPrimitive(e.typ.Primitive, value)
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
						value := values[0].([]byte)
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
