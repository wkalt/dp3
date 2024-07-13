package util_test

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wkalt/dp3/server/util"
)

func mustInt(i any) int {
	x, ok := i.(int)
	if !ok {
		panic("invalid type")
	}
	return x
}

func TestPriorityQueue(t *testing.T) {
	pq := util.NewPriorityQueue(func(a, b int) bool {
		return a < b
	})
	heap.Init(pq)
	heap.Push(pq, 3)
	heap.Push(pq, 2)
	heap.Push(pq, 1)

	assert.Equal(t, 1, mustInt(heap.Pop(pq)))
	assert.Equal(t, 2, mustInt(heap.Pop(pq)))
	assert.Equal(t, 3, mustInt(heap.Pop(pq)))
}
