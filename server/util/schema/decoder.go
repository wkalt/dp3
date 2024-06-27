package schema

/*
The Decoder interface is our mechanism for pluggable message encoding formats.
Each supported encoding must implement a Decoder and a schema definition parser
that transforms a schema into a schema.Schema. Given these, we get record
parsing and JSON transcoding from our existing plumbing.
*/

////////////////////////////////////////////////////////////////////////////////

type Decoder interface { // nolint: interfacebloat
	Bool() (bool, error)
	Int8() (int8, error)
	Int16() (int16, error)
	Int32() (int32, error)
	Int64() (int64, error)
	Uint8() (uint8, error)
	Uint16() (uint16, error)
	Uint32() (uint32, error)
	Uint64() (uint64, error)
	Float32() (float32, error)
	Float64() (float64, error)
	Time() (uint64, error)
	Duration() (uint64, error)
	String() (string, error)
	Char() (byte, error)
	Byte() (byte, error)

	Bytes(n int) ([]byte, error)

	SkipBytes(n int) error

	SkipBool() error
	SkipInt8() error
	SkipInt16() error
	SkipInt32() error
	SkipInt64() error
	SkipUint8() error
	SkipUint16() error
	SkipUint32() error
	SkipUint64() error
	SkipFloat32() error
	SkipFloat64() error
	SkipTime() error
	SkipDuration() error
	SkipString() error
	SkipChar() error
	SkipByte() error

	ArrayLength() (int64, error)
	Set(b []byte)
	Reset()
}
