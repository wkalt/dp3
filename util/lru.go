package util

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var ErrValueTooLarge = errors.New("value is too large")

type LRU[V any] struct {
	cache      map[uint64]*listNode[V]
	head, tail *listNode[V]
	count      int64
	cap        int64
	mtx        *sync.Mutex
}

type listNode[V any] struct {
	key        uint64
	value      V
	prev, next *listNode[V]
}

func newListNode[V any]() *listNode[V] {
	return &listNode[V]{
		key:   0,
		value: *new(V),
		prev:  nil,
		next:  nil,
	}
}

func NewLRU[V any](capacity int64) *LRU[V] {
	if capacity <= 0 {
		panic("cap must be positive")
	}
	head, tail := newListNode[V](), newListNode[V]()
	head.next = tail
	tail.prev = head
	return &LRU[V]{
		cache: make(map[uint64]*listNode[V]),
		head:  head,
		tail:  tail,
		cap:   capacity,
		count: 0,
		mtx:   &sync.Mutex{},
	}
}

func (lru *LRU[V]) Reset() {
	lru.mtx.Lock()
	defer lru.mtx.Unlock()
	lru.cache = make(map[uint64]*listNode[V])
	lru.head.next = lru.tail
	lru.tail.prev = lru.head
	lru.count = 0
}

func (lru *LRU[V]) addToFront(node *listNode[V]) {
	node.next = lru.head.next
	node.prev = lru.head
	lru.head.next.prev = node
	lru.head.next = node
}

func (lru *LRU[V]) removeNode(node *listNode[V]) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (lru *LRU[V]) moveToFront(node *listNode[V]) {
	lru.removeNode(node)
	lru.addToFront(node)
}

func (lru *LRU[V]) Put(key uint64, value V) {
	lru.mtx.Lock()
	defer lru.mtx.Unlock()
	if node, exists := lru.cache[key]; exists {
		node.value = value
		lru.moveToFront(node)
	} else {
		node := &listNode[V]{key: key, value: value, prev: nil, next: nil}
		lru.cache[key] = node
		lru.addToFront(node)
		lru.count++
	}

	for lru.count > lru.cap {
		lru.evict()
	}
}

func (lru *LRU[V]) Get(key uint64) (V, bool) {
	lru.mtx.Lock()
	defer lru.mtx.Unlock()
	if node, exists := lru.cache[key]; exists {
		lru.moveToFront(node)
		return node.value, true
	}
	var v V
	return v, false
}

func (lru *LRU[V]) evict() {
	if lru.tail.prev == lru.head {
		return // Cache is empty
	}
	lru.count--
	delete(lru.cache, lru.tail.prev.key)
	lru.removeNode(lru.tail.prev)
}

func (lru *LRU[V]) String() string {
	lru.mtx.Lock()
	defer lru.mtx.Unlock()
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("(%d/%d) [", lru.count, lru.cap))
	for node := lru.head.next; node != lru.tail; node = node.next {
		sb.WriteString(fmt.Sprintf("%d:%v", node.key, node.value))
		if node.next != lru.tail {
			sb.WriteString(" ")
		}
	}
	sb.WriteString("]")
	return sb.String()
}
