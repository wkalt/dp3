package util_test

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wkalt/dp3/util"
)

func TestPriorityQueue(t *testing.T) {
	pq := util.NewPriorityQueue[int, int]()
	heap.Init(pq)
	heap.Push(pq, &util.Item[int, int]{Value: 1, Priority: 3})
	heap.Push(pq, &util.Item[int, int]{Value: 2, Priority: 2})
	heap.Push(pq, &util.Item[int, int]{Value: 3, Priority: 1})
	assert.Equal(t, 3, heap.Pop(pq).(int))
	assert.Equal(t, 2, heap.Pop(pq).(int))
	assert.Equal(t, 1, heap.Pop(pq).(int))
}
