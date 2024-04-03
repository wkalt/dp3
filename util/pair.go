package util

// Pair is a generic pair of values.
type Pair[A any, B any] struct {
	First  A
	Second B
}

// NewPair creates a new Pair.
func NewPair[A, B any](first A, second B) Pair[A, B] {
	return Pair[A, B]{First: first, Second: second}
}
