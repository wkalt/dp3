package wal

import (
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/wkalt/dp3/util"
)

/*
The WAL writer is used to write records to the write-ahead log. The format of
the write-ahead log is as follows:

    Magic: 6 bytes (dp3wal)
    Version: 2 bytes (major, minor)
    [Record]*

Where each Record is:
    Type: 1 byte
    Length: 8 bytes
    Data: [Length]byte
    CRC32: 4 bytes

The CRC is calculated over all preceding bytes of the record - i.e including the
record type. The type may be "insert", "merge request", or "merge complete".
*/

////////////////////////////////////////////////////////////////////////////////

// Magic is the magic number for the WAL file.
var Magic = []byte{'d', 'p', '3', 'w', 'a', 'l'} // nolint:gochecknoglobals

const (
	currentMajor = uint8(0)
	currentMinor = uint8(0)
)

type Writer struct {
	id     uint64
	writer io.Writer
	offset int64
	mtx    *sync.Mutex
}

// NewWriter creates a new WAL writer.
func NewWriter(w io.Writer, id uint64, initialOffset int64) (*Writer, error) {
	if initialOffset == 0 {
		buf := make([]byte, 8)
		offset := copy(buf, Magic)
		offset += util.U8(buf[offset:], currentMajor)
		util.U8(buf[offset:], currentMinor)
		n, err := w.Write(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to write WAL magic: %w", err)
		}
		initialOffset = int64(n)
	}
	return &Writer{
		id:     id,
		writer: w,
		mtx:    &sync.Mutex{},
		offset: initialOffset,
	}, nil
}

// WriteMergeComplete writes a merge complete record.
func (w *Writer) WriteMergeComplete(rec MergeCompleteRecord) (Address, int, error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	length := 4 + len(rec.BatchID)
	data := make([]byte, length)
	_ = util.WritePrefixedString(data, rec.BatchID)
	return w.writeRecord(WALMergeComplete, nil, data)
}

// WriteMergeRequest writes a merge request record.
func (w *Writer) WriteMergeRequest(rec MergeRequestRecord) (Address, int, error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	length := 4 + len(rec.Producer) + 4 + len(rec.Topic) + 4 + len(rec.BatchID) + 24*len(rec.Addrs)
	data := make([]byte, length)
	offset := util.WritePrefixedString(data, rec.Producer)
	offset += util.WritePrefixedString(data[offset:], rec.Topic)
	offset += util.WritePrefixedString(data[offset:], rec.BatchID)
	for _, addr := range rec.Addrs {
		offset += copy(data[offset:], addr[:])
	}
	return w.writeRecord(WALMergeRequest, nil, data)
}

// WriteInsert writes an insert record.
func (w *Writer) WriteInsert(rec InsertRecord) (Address, int, error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	header := make([]byte, 4+len(rec.Producer)+4+len(rec.Topic)+4+len(rec.BatchID)+24)
	var offset int
	offset += util.WritePrefixedString(header[offset:], rec.Producer)
	offset += util.WritePrefixedString(header[offset:], rec.Topic)
	offset += util.WritePrefixedString(header[offset:], rec.BatchID)

	addr := NewAddress(w.id, uint64(w.size()), uint64(1+8+len(header)+len(rec.Data)+4))
	copy(header[offset:], addr[:])

	return w.writeRecord(WALInsert, header, rec.Data)
}

func (w *Writer) size() int64 {
	return w.offset
}

// writeRecord writes a record to the WAL.
func (w *Writer) writeRecord(rectype RecordType, header []byte, data []byte) (addr Address, n int, err error) {
	buf := make([]byte, 1+8+len(header)+len(data)+4)
	offset := 0
	offset += util.U8(buf[offset:], uint8(rectype))
	offset += util.U64(buf[offset:], uint64(len(header)+len(data)))
	if len(header) > 0 {
		offset += copy(buf[offset:], header)
	}
	offset += copy(buf[offset:], data)

	crc := crc32.ChecksumIEEE(buf[:offset])
	util.U32(buf[offset:], crc)

	n, err = w.writer.Write(buf)
	w.offset += int64(n)
	if err != nil {
		return addr, n, fmt.Errorf("failed to write record header: %w", err)
	}
	if f, ok := w.writer.(interface{ Flush() error }); ok {
		if err := f.Flush(); err != nil {
			return addr, n, fmt.Errorf("failed to flush writer: %w", err)
		}
	}
	addr = NewAddress(w.id, uint64(w.offset-int64(n)), uint64(n))
	return addr, n, nil
}
