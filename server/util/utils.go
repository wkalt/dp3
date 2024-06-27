package util

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/wkalt/dp3/server/util/log"
)

/*
Utility functions.
*/

////////////////////////////////////////////////////////////////////////////////

// Pow returns x raised to the power of y.
func Pow[V int | int64 | float64 | uint64 | float32](x V, y int) V {
	if y == 0 {
		return 1
	}
	if y == 1 {
		return x
	}
	result := x
	for i := 2; i <= y; i++ {
		result *= x
	}
	return result
}

// GroupBy groups records by the result of f.
func GroupBy[T any, K comparable](records []T, f func(T) K) map[K][]T {
	groups := make(map[K][]T)
	for _, record := range records {
		key := f(record)
		groups[key] = append(groups[key], record)
	}
	return groups
}

// ParseNanos returns a time.Time from a nanosecond timestamp.
func ParseNanos(x uint64) time.Time {
	return time.Unix(int64(x/1e9), int64(x%1e9))
}

// DateSeconds returns a Unix timestamp from a date string.
func DateSeconds(date string) uint64 {
	t, _ := time.Parse("2006-01-02", date)
	return uint64(t.Unix())
}

// Okeys returns the keys of a map in sorted order.
func Okeys[T cmp.Ordered, K any](m map[T]K) []T {
	keys := make([]T, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// HumanBytes returns a human-readable representation of a number of bytes.
func HumanBytes(n uint64) string {
	suffix := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	i := 0
	for n >= 1024 && i < len(suffix)-1 {
		n /= 1024
		i++
	}
	return strconv.FormatUint(n, 10) + " " + suffix[i]
}

// When returns a if cond is true, otherwise b.
func When[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

// Reduce applies a function to each element of a slice, accumulating the
// results.
func Reduce[T any, U any](f func(U, T) U, init U, xs []T) U {
	acc := init
	for _, x := range xs {
		acc = f(acc, x)
	}
	return acc
}

func Filter[T any](f func(T) bool, xs []T) []T {
	ys := make([]T, 0, len(xs))
	for _, x := range xs {
		if f(x) {
			ys = append(ys, x)
		}
	}
	return ys
}

// Map applies a function to each element of a slice, returning a new slice.
func Map[T any, U any](f func(T) U, xs []T) []U {
	ys := make([]U, len(xs))
	for i, x := range xs {
		ys[i] = f(x)
	}
	return ys
}

// Max returns the maximum of a and b.
func Max[T cmp.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// Min returns the minimum of a and b.
func Min[T cmp.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

// CryptographicHash returns a stable hex-encoded cryptographic hash for use in the
// project.
func CryptographicHash(data []byte) string {
	h := sha256.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// Pointer returns a pointer to x.
func Pointer[T any](x T) *T {
	return &x
}

func All[T any](xs []T, f func(T) bool) bool {
	for _, x := range xs {
		if !f(x) {
			return false
		}
	}
	return true
}

func EnsureDirectoryExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to make directory: %w", err)
		}
	}
	return nil
}

func RunPipe(
	ctx context.Context,
	wf func(ctx context.Context, w io.Writer) error,
	rf func(ctx context.Context, r io.Reader) error,
) error {
	r, w := io.Pipe()
	readerrs := make(chan error, 1)
	// spawn a goroutine to run the reader function over the input.
	go func() {
		err := rf(ctx, r)
		if err != nil {
			w.CloseWithError(err)
			readerrs <- err
		} else {
			readerrs <- r.Close()
		}
	}()
	// if the writer encounters an error, close the reader and return.
	if err := wf(ctx, w); err != nil {
		r.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close pipe writer: %w", err)
	}
	// once the writer is done, the reader must be allowed to catch up.
	select {
	case err := <-readerrs:
		if err != nil {
			return fmt.Errorf("error closing pipe reader: %w", err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context error: %w", ctx.Err())
	}
}

// MaybeWarn logs a warning if f returns an error. It is intended to wrap
// deferred Close calls in situations where an error is not critical and would
// not alter program execution. Most often this is the case for readers but not
// writers.
func MaybeWarn(ctx context.Context, f func() error) {
	if err := f(); err != nil {
		log.Warnf(ctx, "warning: %v", err)
	}
}

// CloseAll closes all closers and returns a wrapped error of the first error
// encountered, annotating the result with any additional errors.
func CloseAll[T io.Closer](closers ...T) error {
	errs := make([]error, len(closers))
	for i, c := range closers {
		if err := c.Close(); err != nil {
			errs[i] = err
		}
	}
	errored := Filter(func(e error) bool { return e != nil }, errs)
	if len(errored) > 0 {
		rest := When(
			len(errored) > 1,
			fmt.Sprintf(" (other errors: %s)", strings.Join(
				Map(func(e error) string { return e.Error() }, errored), ", ")),
			"",
		)
		return fmt.Errorf("failed to close resource: %w%s", errored[0], rest)
	}
	return nil
}

// ContextCloser is an interface for objects that can be closed with a context.
type ContextCloser interface {
	Close(ctx context.Context) error
}

// CloseAllContext closes all closers with a context and returns a wrapped error
// of the first error encountered, annotating the result with any additional
// errors.
func CloseAllContext[T ContextCloser](ctx context.Context, closers ...T) error {
	errs := make([]error, len(closers))
	for i, c := range closers {
		if err := c.Close(ctx); err != nil {
			errs[i] = err
		}
	}
	errored := Filter(func(e error) bool { return e != nil }, errs)
	if len(errored) > 0 {
		rest := When(
			len(errored) > 1,
			fmt.Sprintf(" (other errors: %s)", strings.Join(
				Map(func(e error) string { return e.Error() }, errored), ", ")),
			"",
		)
		return fmt.Errorf("failed to close resource: %w%s", errored[0], rest)
	}
	return nil
}

func HashSliceOverlap[T comparable](a, b []T) bool {
	m := make(map[T]struct{}, len(a))
	for _, x := range a {
		m[x] = struct{}{}
	}
	for _, y := range b {
		if _, ok := m[y]; ok {
			return true
		}
	}
	return false
}
