package util

/*
Encoding utilities. Note that these utilities do not check lengths - it is
necessary to ensure buffers passed (to write functions) are large enough and
that parsed string data is valid (i.e via crc content validation first), or a
panic may result.
*/

import "encoding/binary"

// ReadU8 reads a uint8 from src and stores it in x, returning the written length.
func ReadU8(src []byte, x *uint8) int {
	*x = src[0]
	return 1
}

// ReadU32 reads a uint32 from src and stores it in x, returning the written length.
func ReadU32(src []byte, x *uint32) int {
	*x = binary.LittleEndian.Uint32(src)
	return 4
}

// ReadU64 reads a uint64 from src and stores it in x, returning the written length.
func ReadU64(src []byte, x *uint64) int {
	*x = binary.LittleEndian.Uint64(src)
	return 8
}

// ReadPrefixedString reads a string from data and stores it in s, returning the
// written length.
func ReadPrefixedString(data []byte, s *string) int {
	if len(data) < 4 {
		panic("short buffer")
	}
	length := int(binary.LittleEndian.Uint32(data))
	if len(data[4:]) < length {
		panic("short buffer")
	}
	*s = string(data[4 : length+4])
	return 4 + length
}

// ReadPrefixedString reads a string from src and stores it in x, returning the written length.
func U8(dst []byte, src uint8) int {
	dst[0] = src
	return 1
}

// U32 writes a uint32 to dst and returns the written length.
func U32(dst []byte, src uint32) int {
	binary.LittleEndian.PutUint32(dst, src)
	return 4
}

// U64 writes a uint64 to dst and returns the written length.
func U64(dst []byte, src uint64) int {
	binary.LittleEndian.PutUint64(dst, src)
	return 8
}

// WritePrefixedString writes a string to buf and returns the written length.
func WritePrefixedString(buf []byte, s string) int {
	if len(buf) < 4+len(s) {
		panic("buffer too small")
	}
	binary.LittleEndian.PutUint32(buf, uint32(len(s)))
	return 4 + copy(buf[4:], s)
}
