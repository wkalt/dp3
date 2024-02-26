package util

import (
	"errors"
	"fmt"
	"strings"
)

var ErrValueTooLarge = errors.New("value is too large")

type LRU struct {
	cache        map[uint64]*listNode
	head, tail   *listNode
	byteCount    int64
	byteCapacity int64
}

type listNode struct {
	key        uint64
	value      []byte
	prev, next *listNode
}

func NewLRU(byteCapacity int64) *LRU {
	if byteCapacity <= 0 {
		panic("byteCapacity must be positive")
	}
	head, tail := &listNode{}, &listNode{}
	head.next = tail
	tail.prev = head
	return &LRU{
		cache:        make(map[uint64]*listNode),
		head:         head,
		tail:         tail,
		byteCapacity: byteCapacity,
	}
}

func (lru *LRU) addToFront(node *listNode) {
	node.next = lru.head.next
	node.prev = lru.head
	lru.head.next.prev = node
	lru.head.next = node
}

func (lru *LRU) removeNode(node *listNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (lru *LRU) moveToFront(node *listNode) {
	lru.removeNode(node)
	lru.addToFront(node)
}

func (lru *LRU) Put(key uint64, value []byte) error {
	if int64(len(value)) > lru.byteCapacity {
		return ErrValueTooLarge
	}
	if node, exists := lru.cache[key]; exists {
		lru.byteCount += int64(len(value)) - int64(len(node.value))
		node.value = value
		lru.moveToFront(node)
	} else {
		node := &listNode{key: key, value: value}
		lru.cache[key] = node
		lru.addToFront(node)
		lru.byteCount += int64(len(value))
	}

	for lru.byteCount > lru.byteCapacity {
		lru.evict()
	}
	return nil
}

func (lru *LRU) Get(key uint64) ([]byte, bool) {
	if node, exists := lru.cache[key]; exists {
		lru.moveToFront(node)
		return node.value, true
	}
	return nil, false
}

func (lru *LRU) evict() {
	if lru.tail.prev == lru.head {
		return // Cache is empty
	}
	lru.byteCount -= int64(len(lru.tail.prev.value))
	delete(lru.cache, lru.tail.prev.key)
	lru.removeNode(lru.tail.prev)
}

func (lru *LRU) String() string {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("(%d/%d) [", lru.byteCount, lru.byteCapacity))
	for node := lru.head.next; node != lru.tail; node = node.next {
		sb.WriteString(fmt.Sprintf("%d:%d", node.key, len(node.value)))
		if node.next != lru.tail {
			sb.WriteString(" ")
		}
	}
	sb.WriteString("]")
	return sb.String()
}
