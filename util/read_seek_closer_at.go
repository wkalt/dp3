package util

import (
	"fmt"
	"io"
)

type readSeekCloserAt struct {
	rsc io.ReadSeekCloser

	offset int
	length int

	pos int
}

func NewReadSeekCloserAt(rsc io.ReadSeekCloser, offset, length int) (io.ReadSeekCloser, error) {
	_, err := rsc.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to offset: %w", err)
	}
	return &readSeekCloserAt{
		rsc:    rsc,
		offset: offset,
		length: length,
	}, nil
}

func (rsc *readSeekCloserAt) Read(p []byte) (n int, err error) {
	if rsc.pos == rsc.length {
		return 0, io.EOF
	}
	if rsc.pos+len(p) > rsc.length {
		short := p[:rsc.length-rsc.pos]
		n, err := rsc.rsc.Read(short)
		rsc.pos += n
		if err != nil {
			return n, fmt.Errorf("read failure: %w", err)
		}
		if n < len(short) {
			return n, nil
		}
		return n, nil
	}
	n, err = rsc.rsc.Read(p)
	rsc.pos += n
	if err != nil {
		return n, fmt.Errorf("read failure: %w", err)
	}
	return n, nil
}

func (rsc *readSeekCloserAt) seek(offset int64) (int64, error) {
	n, err := rsc.rsc.Seek(offset, io.SeekStart)
	if err != nil {
		return n, fmt.Errorf("seek failure: %w", err)
	}
	return n, nil
}

func (rsc *readSeekCloserAt) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		translated := int(offset) + rsc.offset
		rsc.pos = int(offset)
		if translated < rsc.offset || translated > rsc.offset+rsc.length {
			return 0, fmt.Errorf("invalid offset for seek start: %d", translated)
		}
		return rsc.seek(int64(translated))
	case io.SeekCurrent:
		translated := rsc.offset + rsc.pos + int(offset)
		rsc.pos += int(offset)
		if translated < rsc.offset || translated > rsc.offset+rsc.length {
			return 0, fmt.Errorf("invalid offset for seek current: %d", translated)
		}
		return rsc.seek(int64(translated))
	case io.SeekEnd:
		translated := rsc.offset + rsc.length + int(offset)
		rsc.pos = rsc.length + int(offset)
		if translated < rsc.offset || translated > rsc.offset+rsc.length {
			return 0, fmt.Errorf("invalid offset for seek end: %d", translated)
		}
		return rsc.seek(int64(translated))
	default:
		panic("invalid whence")
	}
}

func (rsc *readSeekCloserAt) Close() error {
	if err := rsc.rsc.Close(); err != nil {
		return fmt.Errorf("failed to close underlying reader: %w", err)
	}
	return nil
}
