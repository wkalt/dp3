package util

import "fmt"

type Named[T any] struct {
	Name  string `json:"name"`
	Value T      `json:"data"`
}

func (n Named[T]) String() string {
	return fmt.Sprintf("(%s: %v", n.Name, n.Value)
}

func NewNamed[T any](name string, data T) Named[T] {
	return Named[T]{Name: name, Value: data}
}
