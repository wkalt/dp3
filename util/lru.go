package util

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var ErrValueTooLarge = errors.New("value is too large")

// LRU is a simple LRU cache.
type LRU[K comparable, V any] struct {
	cache      map[K]*listNode[K, V]
	head, tail *listNode[K, V]
	size       uint64
	cap        uint64
	mtx        *sync.Mutex
}

type listNode[K comparable, V any] struct {
	key        K
	value      V
	size       uint64
	prev, next *listNode[K, V]
}

func newListNode[K comparable, V any]() *listNode[K, V] {
	return &listNode[K, V]{
		key:   *new(K),
		value: *new(V),
		size:  0,
		prev:  nil,
		next:  nil,
	}
}

// NewLRU returns a new LRU cache with the given capacity.
func NewLRU[K comparable, V any](capacity uint64) *LRU[K, V] {
	head, tail := newListNode[K, V](), newListNode[K, V]()
	head.next = tail
	tail.prev = head
	return &LRU[K, V]{
		cache: make(map[K]*listNode[K, V]),
		head:  head,
		tail:  tail,
		cap:   capacity,
		size:  0,
		mtx:   &sync.Mutex{},
	}
}

// Reset clears the cache.
func (lru *LRU[K, V]) Reset() {
	lru.mtx.Lock()
	defer lru.mtx.Unlock()
	lru.cache = make(map[K]*listNode[K, V])
	lru.head.next = lru.tail
	lru.tail.prev = lru.head
	lru.size = 0
}

func (lru *LRU[K, V]) addToFront(node *listNode[K, V]) {
	node.next = lru.head.next
	node.prev = lru.head
	lru.head.next.prev = node
	lru.head.next = node
}

func (lru *LRU[K, V]) removeNode(node *listNode[K, V]) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (lru *LRU[K, V]) moveToFront(node *listNode[K, V]) {
	lru.removeNode(node)
	lru.addToFront(node)
}

// Put adds a new key-value pair to the cache. If the key already exists, the value is updated.
func (lru *LRU[K, V]) Put(key K, value V, size uint64) {
	lru.mtx.Lock()
	defer lru.mtx.Unlock()
	if node, exists := lru.cache[key]; exists {
		node.value = value
		lru.moveToFront(node)
	} else {
		node := &listNode[K, V]{key: key, value: value, size: size, prev: nil, next: nil}
		lru.cache[key] = node
		lru.addToFront(node)
		lru.size += size
	}

	for lru.size > lru.cap {
		lru.evict()
	}
}

// Get returns the value associated with the given key. The second return value is true if the key exists in the cache.
func (lru *LRU[K, V]) Get(key K) (V, bool) {
	lru.mtx.Lock()
	defer lru.mtx.Unlock()
	if node, exists := lru.cache[key]; exists {
		lru.moveToFront(node)
		return node.value, true
	}
	var v V
	return v, false
}

func (lru *LRU[K, V]) evict() {
	if lru.tail.prev == lru.head {
		return // Cache is empty
	}
	delete(lru.cache, lru.tail.prev.key)
	tailsize := lru.tail.prev.size
	lru.removeNode(lru.tail.prev)
	lru.size -= tailsize
}

// String returns a string representation of the cache.
func (lru *LRU[K, V]) String() string {
	lru.mtx.Lock()
	defer lru.mtx.Unlock()
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("(%d/%d) [", lru.size, lru.cap))
	for node := lru.head.next; node != lru.tail; node = node.next {
		sb.WriteString(fmt.Sprintf("%v:%v", node.key, node.value))
		if node.next != lru.tail {
			sb.WriteString(" ")
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func (lru *LRU[K, V]) Size() (int, uint64) {
	lru.mtx.Lock()
	defer lru.mtx.Unlock()
	return len(lru.cache), lru.size
}
