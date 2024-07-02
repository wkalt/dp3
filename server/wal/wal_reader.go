package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/util"
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
	var database, producer, topic, batchID string
	offset += util.ReadPrefixedString(data[offset:], &database)
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
		Database: database,
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
	var database, producer, topic, batchID string
	offset += util.ReadPrefixedString(data[offset:], &database)
	offset += util.ReadPrefixedString(data[offset:], &producer)
	offset += util.ReadPrefixedString(data[offset:], &topic)
	offset += util.ReadPrefixedString(data[offset:], &batchID)

	var schemaCount uint32
	offset += util.ReadU32(data[offset:], &schemaCount)
	schemas := make([]*mcap.Schema, schemaCount)
	for i := range schemas {
		schema := &mcap.Schema{}
		offset += util.ReadPrefixedString(data[offset:], &schema.Name)
		offset += util.ReadPrefixedString(data[offset:], &schema.Encoding)
		offset += util.ReadPrefixedBytes(data[offset:], &schema.Data)
		schemas[i] = schema
	}

	var addr Address
	offset += copy(addr[:], data[offset:offset+24])
	return &InsertRecord{
		Database: database,
		Producer: producer,
		Topic:    topic,
		BatchID:  batchID,
		Addr:     addr,
		Schemas:  schemas,
		Data:     data[offset:],
	}
}

func ParseInsertRecordHeader(r io.Reader) (*InsertRecord, int, error) {
	buf := make([]byte, 9)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read insert record header: %w", err)
	}
	database, err := util.DecodePrefixedString(r)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse database: %w", err)
	}
	producer, err := util.DecodePrefixedString(r)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse producer: %w", err)
	}
	topic, err := util.DecodePrefixedString(r)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse topic: %w", err)
	}
	batchID, err := util.DecodePrefixedString(r)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse batch ID: %w", err)
	}
	_, err = io.ReadFull(r, buf[:4])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read schema count: %w", err)
	}
	schemaCount := binary.LittleEndian.Uint32(buf)
	schemas := make([]*mcap.Schema, schemaCount)
	schemasLength := 4 // four byte count
	for i := 0; i < int(schemaCount); i++ {
		name, err := util.DecodePrefixedString(r)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse schema name: %w", err)
		}
		encoding, err := util.DecodePrefixedString(r)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse schema encoding: %w", err)
		}
		data, err := util.DecodePrefixedBytes(r)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse schema data: %w", err)
		}
		schemas[i] = &mcap.Schema{Name: name, Encoding: encoding, Data: data}
		schemasLength += 4 + len(name) + 4 + len(encoding) + 4 + len(data)
	}
	record := &InsertRecord{
		Database: database,
		Producer: producer,
		Topic:    topic,
		BatchID:  batchID,
		Schemas:  schemas,
	}
	return record, 24 +
		n +
		4*4 +
		len(database) +
		len(producer) +
		len(topic) +
		len(batchID) +
		schemasLength, nil
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
