package util

import (
	"cmp"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"
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

// ComputeStreamID returns a unique stream ID from a hashid and a topic.
func ComputeStreamID(hashid string, topic string) string {
	sum := md5.Sum([]byte(hashid + topic))
	return hex.EncodeToString(sum[:])
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

// CryptoHash returns a stable hex-encoded cryptographic hash for use in the
// project.
func CryptoHash(data []byte) string {
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
