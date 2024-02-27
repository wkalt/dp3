package util_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wkalt/dp3/util"
)

func TestLRU(t *testing.T) {
	t.Run("simple inserts", func(t *testing.T) {
		lru := util.NewLRU[string](100)
		lru.Put(1, "a")
		lru.Put(2, "a")
		lru.Put(3, "a")
		assert.Equal(t, "(3/100) [3:a 2:a 1:a]", lru.String())
	})
	t.Run("eviction", func(t *testing.T) {
		lru := util.NewLRU[string](2)
		lru.Put(1, "a")
		lru.Put(2, "a")
		lru.Put(3, "a")
		assert.Equal(t, "(2/2) [3:a 2:a]", lru.String())
	})
	t.Run("get key that does not exist", func(t *testing.T) {
		lru := util.NewLRU[string](100)
		_, ok := lru.Get(1)
		assert.False(t, ok)
	})
	t.Run("reset the cache", func(t *testing.T) {
		lru := util.NewLRU[string](100)
		lru.Put(1, "a")
		lru.Put(2, "a")
		lru.Put(3, "a")
		lru.Reset()
		assert.Equal(t, "(0/100) []", lru.String())
	})
	t.Run("get moves items to front", func(t *testing.T) {
		lru := util.NewLRU[string](100)
		lru.Put(1, "a")
		lru.Put(2, "a")
		lru.Put(3, "a")
		_, ok := lru.Get(1)
		assert.True(t, ok)
		assert.Equal(t, "(3/100) [1:a 3:a 2:a]", lru.String())
	})
	t.Run("overwrite moves item to the front", func(t *testing.T) {
		lru := util.NewLRU[string](100)
		lru.Put(1, "a")
		lru.Put(2, "a")
		lru.Put(1, "ab")
		_, ok := lru.Get(1)
		assert.True(t, ok)
		assert.Equal(t, "(2/100) [1:ab 2:a]", lru.String())
	})
}
