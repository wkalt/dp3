package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLRU(t *testing.T) {
	t.Run("simple inserts", func(t *testing.T) {
		lru := NewLRU(100)
		assert.Nil(t, lru.Put(1, []byte("a")))
		assert.Nil(t, lru.Put(2, []byte("a")))
		assert.Nil(t, lru.Put(3, []byte("a")))
		assert.Equal(t, "(3/100) [3:1 2:1 1:1]", lru.String())
	})
	t.Run("eviction", func(t *testing.T) {
		lru := NewLRU(100)
		assert.Nil(t, lru.Put(1, []byte("a")))
		assert.Nil(t, lru.Put(2, []byte("a")))
		assert.Nil(t, lru.Put(3, make([]byte, 100)))
		assert.Equal(t, "(100/100) [3:100]", lru.String())
	})
	t.Run("oversized items are disallowed", func(t *testing.T) {
		lru := NewLRU(100)
		lru.Put(1, []byte("a"))
		assert.ErrorIs(t, lru.Put(1, make([]byte, 1000)), ErrValueTooLarge)
		assert.Equal(t, "(1/100) [1:1]", lru.String())
	})
	t.Run("get moves items to front", func(t *testing.T) {
		lru := NewLRU(100)
		assert.Nil(t, lru.Put(1, []byte("a")))
		assert.Nil(t, lru.Put(2, []byte("a")))
		assert.Nil(t, lru.Put(3, []byte("a")))
		_, ok := lru.Get(1)
		assert.True(t, ok)
		assert.Equal(t, "(3/100) [1:1 3:1 2:1]", lru.String())
	})
	t.Run("overwrite moves item to the front", func(t *testing.T) {
		lru := NewLRU(100)
		assert.Nil(t, lru.Put(1, []byte("a")))
		assert.Nil(t, lru.Put(2, []byte("a")))
		assert.Nil(t, lru.Put(1, []byte("ab")))
		_, ok := lru.Get(1)
		assert.True(t, ok)
		assert.Equal(t, "(3/100) [1:2 2:1]", lru.String())
	})
}
