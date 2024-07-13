package util

import (
	"errors"
	"fmt"
	"io"
)

// WrappedReadSeekCloser returns a new logically-bounded ReadSeekCloser within the
// provided one. Calling Seek(0, io.SeekStart) will seek to the inset in the
// surrounding file, not zero. Seeking beyond the length of the inset will
// return io.EOF.
type WrappedReadSeekCloser struct {
	rsc    io.ReadSeekCloser
	inset  int64
	length int64

	logicalPos int64
}

// NewWrappedReadSeekCloser returns a new InsetReadSeekCloser.
func NewWrappedReadSeekCloser(rsc io.ReadSeekCloser, physicalInset, length int64) *WrappedReadSeekCloser {
	return &WrappedReadSeekCloser{
		rsc:    rsc,
		inset:  physicalInset,
		length: length,

		logicalPos: 0,
	}
}

// Read reads from the inset.
func (rsc *WrappedReadSeekCloser) Read(p []byte) (n int, err error) {
	if rsc.logicalPos == rsc.length {
		return 0, io.EOF
	}

	if rsc.logicalPos+int64(len(p)) > rsc.length {
		short := p[:rsc.length-rsc.logicalPos]
		n, err := rsc.rsc.Read(short)
		rsc.logicalPos += int64(n)
		if err != nil {
			return n, err // nolint: wrapcheck
		}
		if n < len(short) {
			return n, nil
		}
		return n, nil
	}

	n, err = rsc.rsc.Read(p)
	rsc.logicalPos += int64(n)
	if err != nil {
		return n, err // nolint: wrapcheck
	}
	return n, nil
}

func (rsc *WrappedReadSeekCloser) Close() error {
	if err := rsc.rsc.Close(); err != nil {
		return fmt.Errorf("close failed: %w", err)
	}
	return nil
}

// Seek seeks within the inset.
func (rsc *WrappedReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		pnew, err := rsc.rsc.Seek(rsc.inset+offset, io.SeekStart)
		lnew := pnew - rsc.inset
		if err != nil {
			return lnew, fmt.Errorf("seek failed: %w", err)
		}
		rsc.logicalPos = lnew
		return lnew, nil
	case io.SeekCurrent:
		pnew, err := rsc.rsc.Seek(offset, io.SeekCurrent)
		lnew := pnew - rsc.inset
		if err != nil {
			return lnew, fmt.Errorf("seek failed: %w", err)
		}
		rsc.logicalPos = lnew
		return lnew, nil
	case io.SeekEnd:
		pnew, err := rsc.rsc.Seek(rsc.inset+rsc.length+offset, io.SeekStart)
		lnew := pnew - rsc.inset
		if err != nil {
			return lnew, fmt.Errorf("seek failed: %w", err)
		}
		rsc.logicalPos = lnew
		return lnew, nil
	default:
		return 0, errors.New("unsupported whence")
	}
}
