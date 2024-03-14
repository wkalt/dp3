package testutils

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
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

func U64b(v uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, v)
	return buf
}

func F32b(v float32) []byte {
	return U32b(math.Float32bits(v))
}

func F64b(v float64) []byte {
	return U64b(math.Float64bits(v))
}
