package schema

// Generic schema types applicable to multiple encodings.
type PrimitiveType int

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
	BOOL
	TIME
	DURATION
	CHAR
	BYTE
)

type Type struct {
	Primitive PrimitiveType

	// If it's an array...
	Array     bool
	FixedSize int
	Items     *Type

	// If it's a record...
	Record bool
	Fields []Field
}

type Field struct {
	Name string
	Type Type
}

type Schema struct {
	Name   string
	Fields []Field
}
