package schema

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
}

// Field is a generic representation of a message field.
type Field struct {
	Name string
	Type Type
}

// Schema is a generic representation of a message schema.
type Schema struct {
	Name   string
	Fields []Field
}
