package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/wkalt/dp3/util"
)

/*
The WAL reader provides an iterator interface for reading records from a WAL
file. It is used during recovery on startup and not thereafter (not for address
lookups).
*/

////////////////////////////////////////////////////////////////////////////////

type walReader struct {
	r      io.ReadSeeker
	offset int64
}

func (r *walReader) Offset() int64 {
	return r.offset
}

// NewReader creates a new WAL reader.
func NewReader(r io.ReadSeeker) (*walReader, error) {
	if err := validateMagic(r); err != nil {
		return nil, err
	}
	offset := int64(len(Magic) + 2)
	return &walReader{r: r, offset: offset}, nil
}

// ParseMergeRequestRecord parses a merge request record.
func ParseMergeRequestRecord(data []byte) *MergeRequestRecord {
	offset := 0
	var producer, topic, batchID string
	offset += util.ReadPrefixedString(data[offset:], &producer)
	offset += util.ReadPrefixedString(data[offset:], &topic)
	offset += util.ReadPrefixedString(data[offset:], &batchID)
	var addrs []Address
	for offset < len(data) {
		var a Address
		offset += copy(a[:], data[offset:offset+24])
		addrs = append(addrs, a)
	}
	return &MergeRequestRecord{
		Producer: producer,
		Topic:    topic,
		BatchID:  batchID,
		Addrs:    addrs,
	}
}

// ParseMergeCompleteRecord parses a merge complete record.
func ParseMergeCompleteRecord(data []byte) *MergeCompleteRecord {
	var id string
	_ = util.ReadPrefixedString(data, &id)
	return &MergeCompleteRecord{BatchID: id}
}

// ParseInsertRecord parses an insert record.
func ParseInsertRecord(data []byte) *InsertRecord {
	offset := 0
	var producer, topic, batchID string
	offset += util.ReadPrefixedString(data[offset:], &producer)
	offset += util.ReadPrefixedString(data[offset:], &topic)
	offset += util.ReadPrefixedString(data[offset:], &batchID)

	var addr Address
	offset += copy(addr[:], data[offset:offset+24])
	return &InsertRecord{
		Producer: producer,
		Topic:    topic,
		BatchID:  batchID,
		Addr:     addr,
		Data:     data[offset:],
	}
}

// Next returns the next record from the WAL file, with CRC validation.
func (r *walReader) Next() (RecordType, []byte, error) {
	header := make([]byte, 1+8)
	_, err := io.ReadFull(r.r, header)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read record header: %w", err)
	}
	var offset int
	var rectype uint8
	var length uint64
	offset += util.ReadU8(header[offset:], &rectype)
	util.ReadU64(header[offset:], &length)
	body := make([]byte, length+4)
	_, err = io.ReadFull(r.r, body)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read record body: %w", err)
	}
	dataEnd := len(body) - 4
	computed := crc32.ChecksumIEEE(header)
	computed = crc32.Update(computed, crc32.IEEETable, body[:dataEnd])
	crc := binary.LittleEndian.Uint32(body[dataEnd:])
	if crc != computed {
		return 0, nil, CRCMismatchError{crc, computed}
	}
	r.offset += int64(1 + 8 + len(body))
	return RecordType(rectype), body[:dataEnd], nil
}

func validateMagic(r io.Reader) error {
	buf := make([]byte, 8)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return ErrBadMagic
		}
		return fmt.Errorf("failed to read WAL magic: %w", err)
	}
	if !bytes.Equal(buf[:6], Magic) {
		return ErrBadMagic
	}
	major := buf[6]
	minor := buf[7]
	if major > currentMajor || (major == currentMajor && minor > currentMinor) {
		return UnsupportedWALError{major, minor}
	}
	return nil
}
