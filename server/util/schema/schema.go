package schema

import (
	"fmt"
)

/*
Schema is a generic representation of message schemas. We translate ROS1 schemas
into these. Hopefully we will be able to leverage this for additional encodings,
but probably we can expect various changes to be required.

A schema is an association of a name and a list of fields. Fields can have
primitive or complex types. The usual range of primitives is supported, as well
as fixed-size and variable-length arrays of primitives or complex types.

Arrays of arrays are not supported in ros1msg, so are not something our tests
currently encounter. Probably some encoding will require them though and I don't
think they won't require model changes.
*/

////////////////////////////////////////////////////////////////////////////////

// PrimitiveType is an enumeration of the primitive types.
type PrimitiveType int

func (p PrimitiveType) String() string {
	switch p {
	case INT8:
		return "int8"
	case INT16:
		return "int16"
	case INT32:
		return "int32"
	case INT64:
		return "int64"
	case UINT8:
		return "uint8"
	case UINT16:
		return "uint16"
	case UINT32:
		return "uint32"
	case UINT64:
		return "uint64"
	case FLOAT32:
		return "float32"
	case FLOAT64:
		return "float64"
	case STRING:
		return "string"
	case WSTRING:
		return "wstring"
	case BOOL:
		return "bool"
	case TIME:
		return "time"
	case DURATION:
		return "duration"
	case CHAR:
		return "char"
	case BYTE:
		return "byte"
	default:
		return "unknown"
	}
}

// MarshalJSON returns the JSON representation of the primitive type.
func (p PrimitiveType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, p.String())), nil
}

func (p *PrimitiveType) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case `"int8"`:
		*p = INT8
	case `"int16"`:
		*p = INT16
	case `"int32"`:
		*p = INT32
	case `"int64"`:
		*p = INT64
	case `"uint8"`:
		*p = UINT8
	case `"uint16"`:
		*p = UINT16
	case `"uint32"`:
		*p = UINT32
	case `"uint64"`:
		*p = UINT64
	case `"float32"`:
		*p = FLOAT32
	case `"float64"`:
		*p = FLOAT64
	case `"string"`:
		*p = STRING
	case `"bool"`:
		*p = BOOL
	case `"time"`:
		*p = TIME
	case `"duration"`:
		*p = DURATION
	case `"char"`:
		*p = CHAR
	case `"byte"`:
		*p = BYTE
	default:
		return fmt.Errorf("unknown primitive type: %s", data)
	}
	return nil
}

const (
	INT8 PrimitiveType = iota + 1
	INT16
	INT32
	INT64
	UINT8
	UINT16
	UINT32
	UINT64
	FLOAT32
	FLOAT64
	STRING
	WSTRING
	BOOL
	TIME
	DURATION
	CHAR
	BYTE
)

// Type is a generic representation of a message type.
type Type struct {
	Primitive PrimitiveType

	// If it's an array...
	Array     bool
	FixedSize int
	Items     *Type

	// If it's a record...
	Record bool
	Fields []Field

	// Bounded-size arrays and strings are supported.
	Bounded   bool
	SizeBound int
}

// IsPrimitive returns true if the type is a primitive type.
func (t Type) IsPrimitive() bool {
	return t.Primitive > 0
}

// Field is a generic representation of a message field.
type Field struct {
	Name    string
	Type    Type
	Default any
}

// Schema is a generic representation of a message schema.
type Schema struct {
	Name   string
	Fields []Field
}
