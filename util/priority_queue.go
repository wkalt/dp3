package util

/*
PriorityQueue is a simple heap-based priority queue. We use it to execute
streaming merges over tree iterators.
*/

////////////////////////////////////////////////////////////////////////////////

type PriorityQueue[K comparable] struct {
	items []K
	less  func(a, b K) bool
}

func NewPriorityQueue[K comparable](less func(a, b K) bool) *PriorityQueue[K] {
	items := make([]K, 0)
	return &PriorityQueue[K]{
		items: items,
		less:  less,
	}
}

func (pq PriorityQueue[_]) Len() int {
	return len(pq.items)
}

func (pq PriorityQueue[_]) Less(i, j int) bool {
	return pq.less(pq.items[i], pq.items[j])
}

func (pq PriorityQueue[_]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *PriorityQueue[K]) Push(item any) {
	value, ok := item.(K)
	if !ok {
		panic("pq: invalid item type")
	}
	pq.items = append(pq.items, value)
}

func (pq *PriorityQueue[K]) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	pq.items = old[0 : n-1]
	return item
}
