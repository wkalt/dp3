package util

import "cmp"

/*
PriorityQueue is a simple heap-based priority queue. We use it to execute
streaming merges over tree iterators.
*/

////////////////////////////////////////////////////////////////////////////////

type Item[K comparable, P cmp.Ordered] struct {
	Value    K
	Priority P
}

type PriorityQueue[K comparable, P cmp.Ordered] []*Item[K, P]

func NewPriorityQueue[K comparable, P cmp.Ordered]() *PriorityQueue[K, P] {
	pq := make(PriorityQueue[K, P], 0)
	return &pq
}

func (pq PriorityQueue[_, _]) Len() int {
	return len(pq)
}

func (pq PriorityQueue[_, _]) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue[_, _]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue[K, P]) Push(item any) {
	value, ok := item.(*Item[K, P])
	if !ok {
		panic("invalid type")
	}
	*pq = append(*pq, value)
}

func (pq *PriorityQueue[K, P]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item.Value
}
