package ros1msg

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/wkalt/dp3/util/schema"
)

type ParseFailureError struct {
	Type   string
	Length int
	Offset int
	Err    error
}

func (f ParseFailureError) Error() string {
	return fmt.Sprintf("failed to parse %d byte %s at %d: %v", f.Length, f.Type, f.Offset, f.Err)
}

type parser struct {
	offset int
	buf    []byte
}

func NewDecoder(buf []byte) schema.Decoder {
	return &parser{buf: buf}
}

func (p *parser) Bool() (bool, error) {
	b, err := p.Uint8()
	return b != 0, err
}

func (p *parser) Int8() (int8, error) {
	if p.offset+1 > len(p.buf) {
		return 0, io.ErrShortBuffer
	}
	p.offset++
	return int8(p.buf[p.offset-1]), nil
}

func (p *parser) Int16() (int16, error) {
	if p.offset+2 > len(p.buf) {
		return 0, io.ErrShortBuffer
	}
	x := binary.LittleEndian.Uint16(p.buf[p.offset : p.offset+2])
	p.offset += 2
	return int16(x), nil
}

func unsafeInt32FromBytes(bts []byte) int32 {
	return *(*int32)(unsafe.Pointer(&bts[0]))
}

func (p *parser) Int32() (int32, error) {
	if p.offset+4 > len(p.buf) {
		return 0, io.ErrShortBuffer
	}
	x := unsafeInt32FromBytes(p.buf[p.offset : p.offset+4])
	p.offset += 4
	return x, nil
}

func (p *parser) Int64() (int64, error) {
	if p.offset+8 > len(p.buf) {
		return 0, io.ErrShortBuffer
	}
	x := binary.LittleEndian.Uint64(p.buf[p.offset : p.offset+8])
	p.offset += 8
	return int64(x), nil
}

func (p *parser) Uint8() (uint8, error) {
	if p.offset+1 > len(p.buf) {
		return 0, io.ErrShortBuffer
	}
	p.offset++
	return p.buf[p.offset-1], nil
}

func unsafeUint16FromBytes(bts []byte) uint16 {
	return *(*uint16)(unsafe.Pointer(&bts[0]))
}

func (p *parser) Uint16() (uint16, error) {
	if p.offset+2 > len(p.buf) {
		return 0, io.ErrShortBuffer
	}
	x := unsafeUint16FromBytes(p.buf[p.offset : p.offset+2])
	p.offset += 2
	return x, nil
}

func unsafeUint32FromBytes(bts []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&bts[0]))
}

func (p *parser) Uint32() (uint32, error) {
	if p.offset+4 > len(p.buf) {
		return 0, io.ErrShortBuffer
	}
	x := unsafeUint32FromBytes(p.buf[p.offset : p.offset+4])
	p.offset += 4
	return x, nil
}

func unsafeUint64FromBytes(bts []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&bts[0]))
}

func (p *parser) Uint64() (uint64, error) {
	if p.offset+8 > len(p.buf) {
		return 0, io.ErrShortBuffer
	}
	x := unsafeUint64FromBytes(p.buf[p.offset : p.offset+8])
	p.offset += 8
	return x, nil
}

func unsafeBytesToF32(bts []byte) float32 {
	return *(*float32)(unsafe.Pointer(&bts[0]))
}

func (p *parser) Float32() (float32, error) {
	if p.offset+4 > len(p.buf) {
		return 0, io.ErrShortBuffer
	}
	f := unsafeBytesToF32(p.buf[p.offset : p.offset+4])
	p.offset += 4
	return f, nil
}

func unsafeBytesToF64(bts []byte) float64 {
	return *(*float64)(unsafe.Pointer(&bts[0]))
}

func (p *parser) Float64() (float64, error) {
	n := p.offset + 8
	if n > len(p.buf) {
		return 0, io.ErrShortBuffer
	}
	f := unsafeBytesToF64(p.buf[p.offset:n])
	p.offset = n
	return f, nil
}

func (p *parser) Time() (uint64, error) {
	x, err := p.Uint64()
	if err != nil {
		return 0, err
	}
	nsecs := uint32(x >> 32)
	secs := uint32(x)
	return 1e9*uint64(secs) + uint64(nsecs), nil
}

func (p *parser) Duration() (uint64, error) {
	return p.Time()
}

func (p *parser) Bytes(n int) ([]byte, error) {
	if p.offset+n > len(p.buf) {
		return nil, io.ErrShortBuffer
	}
	data := p.buf[p.offset : p.offset+n]
	p.offset += n
	return data, nil
}

func (p *parser) SkipBytes(n int) error {
	if p.offset+n > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset += n
	return nil
}

func (p *parser) String() (string, error) {
	length, err := p.Uint32()
	if err != nil {
		return "", err
	}
	n := p.offset + int(length)
	if n > len(p.buf) {
		return "", ParseFailureError{"string", n, p.offset - 4, io.ErrShortBuffer}
	}
	s := string(p.buf[p.offset:n])
	p.offset = n
	return s, nil
}

func (p *parser) Char() (byte, error) {
	return p.Uint8()
}

func (p *parser) Byte() (byte, error) {
	return p.Uint8()
}

func (p *parser) SkipInt8() error {
	if p.offset+1 > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset++
	return nil
}

func (p *parser) SkipInt16() error {
	if p.offset+2 > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset += 2
	return nil
}

func (p *parser) SkipInt32() error {
	if p.offset+4 > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset += 4
	return nil
}

func (p *parser) SkipInt64() error {
	if p.offset+8 > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset += 8
	return nil
}

func (p *parser) SkipUint8() error {
	if p.offset+1 > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset++
	return nil
}

func (p *parser) SkipUint16() error {
	if p.offset+2 > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset += 2
	return nil
}

func (p *parser) SkipUint32() error {
	if p.offset+4 > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset += 4
	return nil
}

func (p *parser) SkipUint64() error {
	if p.offset+8 > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset += 8
	return nil
}

func (p *parser) SkipFloat32() error {
	if p.offset+4 > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset += 4
	return nil
}

func (p *parser) SkipFloat64() error {
	if p.offset+8 > len(p.buf) {
		return io.ErrShortBuffer
	}
	p.offset += 8
	return nil
}

func (p *parser) SkipTime() error {
	return p.SkipUint64()
}

func (p *parser) SkipDuration() error {
	return p.SkipUint64()
}

func (p *parser) SkipString() error {
	n, err := p.Uint32()
	if err != nil {
		return err
	}
	if p.offset+int(n) > len(p.buf) {
		return fmt.Errorf("failed to parse prefixed string at %d: %w", p.offset-4, io.ErrShortBuffer)
	}
	p.offset += int(n)
	return nil
}

func (p *parser) SkipChar() error {
	return p.SkipUint8()
}

func (p *parser) SkipByte() error {
	return p.SkipUint8()
}

func (p *parser) SkipBool() error {
	return p.SkipUint8()
}

func (p *parser) Set(buf []byte) {
	p.buf = buf
	p.offset = 0
}

func (p *parser) Reset() {
	p.buf = nil
	p.offset = 0
}
