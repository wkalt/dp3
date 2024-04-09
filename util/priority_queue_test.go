package util_test

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wkalt/dp3/util"
)

func TestPriorityQueue(t *testing.T) {
	pq := util.NewPriorityQueue[int](func(a, b int) bool {
		return a < b
	})
	heap.Init(pq)
	heap.Push(pq, 3)
	heap.Push(pq, 2)
	heap.Push(pq, 1)
	assert.Equal(t, 1, heap.Pop(pq).(int))
	assert.Equal(t, 2, heap.Pop(pq).(int))
	assert.Equal(t, 3, heap.Pop(pq).(int))
}
