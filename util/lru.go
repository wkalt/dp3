package util

import (
	"errors"
	"fmt"
	"strings"
)

var ErrValueTooLarge = errors.New("value is too large")

type LRU[V any] struct {
	cache      map[uint64]*listNode[V]
	head, tail *listNode[V]
	count      int64
	cap        int64
}

type listNode[V any] struct {
	key        uint64
	value      V
	prev, next *listNode[V]
}

func NewLRU[V any](cap int64) *LRU[V] {
	if cap <= 0 {
		panic("cap must be positive")
	}
	head, tail := &listNode[V]{}, &listNode[V]{}
	head.next = tail
	tail.prev = head
	return &LRU[V]{
		cache: make(map[uint64]*listNode[V]),
		head:  head,
		tail:  tail,
		cap:   cap,
	}
}

func (lru *LRU[V]) Reset() {
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
	if node, exists := lru.cache[key]; exists {
		node.value = value
		lru.moveToFront(node)
	} else {
		node := &listNode[V]{key: key, value: value}
		lru.cache[key] = node
		lru.addToFront(node)
		lru.count += 1
	}

	for lru.count > lru.cap {
		lru.evict()
	}
}

func (lru *LRU[V]) Get(key uint64) (V, bool) {
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
	lru.count -= 1
	delete(lru.cache, lru.tail.prev.key)
	lru.removeNode(lru.tail.prev)
}

func (lru *LRU[V]) String() string {
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
