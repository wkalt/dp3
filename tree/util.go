package tree

func pow[V int64 | float64 | uint64 | float32](x V, y int) V {
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

func groupBy[T any, K comparable](records []T, f func(T) K) map[K][]T {
	groups := make(map[K][]T)
	for _, record := range records {
		key := f(record)
		groups[key] = append(groups[key], record)
	}
	return groups
}
