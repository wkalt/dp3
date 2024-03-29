package testutils

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"testing"
)

/*
General purpose test utilitites.
*/

////////////////////////////////////////////////////////////////////////////////

// GetOpenPort returns an open port that can be used for testing.
func GetOpenPort() (int, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, fmt.Errorf("failed to get open port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// Flatten concatenates slices of the same type.
func Flatten[T any](slices ...[]T) []T {
	var result []T
	for _, s := range slices {
		result = append(result, s...)
	}
	return result
}

// U8b returns a byte slice containing a single uint8 value.
func U8b(v uint8) []byte {
	return []byte{v}
}

// U16b returns a byte slice containing a single uint16 value.
func U16b(v uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, v)
	return buf
}

// U32b returns a byte slice containing a single uint32 value.
func U32b(v uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	return buf
}

// U64b returns a byte slice containing a single uint64 value.
func U64b(v uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, v)
	return buf
}

// F32b returns a byte slice containing a single float32 value.
func F32b(v float32) []byte {
	return U32b(math.Float32bits(v))
}

// F64b returns a byte slice containing a single float64 value.
func F64b(v float64) []byte {
	return U64b(math.Float64bits(v))
}

// PrefixedString returns a byte slice containing a string prefixed with its length.
func PrefixedString(s string) []byte {
	buf := make([]byte, 4+len(s))
	binary.LittleEndian.PutUint32(buf, uint32(len(s)))
	copy(buf[4:], s)
	return buf
}

// ReadPrefixedString reads a string from a byte slice.
func ReadPrefixedString(t *testing.T, bs []byte) string {
	t.Helper()
	if len(bs) < 4 {
		t.Fatalf("expected at least 4 bytes, got %d", len(bs))
	}
	l := binary.LittleEndian.Uint32(bs)
	if len(bs) < 4+int(l) {
		t.Fatalf("expected at least %d bytes, got %d", 4+int(l), len(bs))
	}
	return string(bs[4 : 4+l])
}
