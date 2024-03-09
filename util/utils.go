package util

import (
	"cmp"
	"crypto/md5"
	"encoding/hex"
	"slices"
	"strconv"
	"time"
)

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

func GroupBy[T any, K comparable](records []T, f func(T) K) map[K][]T {
	groups := make(map[K][]T)
	for _, record := range records {
		key := f(record)
		groups[key] = append(groups[key], record)
	}
	return groups
}

func ParseNanos(x uint64) time.Time {
	return time.Unix(int64(x/1e9), int64(x%1e9))
}

func DateSeconds(date string) uint64 {
	t, _ := time.Parse("2006-01-02", date)
	return uint64(t.Unix())
}

func ComputeStreamID(hashid string, topic string) string {
	sum := md5.Sum([]byte(hashid + topic))
	return hex.EncodeToString(sum[:])
}

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
