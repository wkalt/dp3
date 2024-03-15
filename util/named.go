package util

type Named[T any] struct {
	Name  string `json:"name"`
	Value T      `json:"data"`
}

func NewNamed[T any](name string, data T) Named[T] {
	return Named[T]{Name: name, Value: data}
}
