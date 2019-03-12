package lru_test

import (
	"testing"

	"github.com/deliveroo/assert-go"
	"github.com/johngibb/batchcache/lru"
)

func TestZeroSizeCache(t *testing.T) {
	c := lru.New(0)
	c.Put(1, "1")
	if n := c.NumEntries(); n != 0 {
		t.Fatalf("cache has %d entries, want zero", n)
	}
}

func TestCacheReplacement(t *testing.T) {
	t.Run("simple replacement", func(t *testing.T) {
		c := lru.New(2)
		c.Put(1, "1")
		c.Put(2, "2")
		c.Put(3, "3")
		c.Put(4, "4")

		// Cache should only have two entries: 1 and 2.
		assert.Equal(t, c.NumEntries(), 2)
		assert.Equal(t, c.Get(1), nil)
		assert.Equal(t, c.Get(2), nil)
		assert.Equal(t, c.Get(3), "3")
		assert.Equal(t, c.Get(4), "4")
	})
	t.Run("get sets last used time", func(t *testing.T) {
		c := lru.New(2)

		c.Put(1, "1")
		c.Put(2, "2")
		_ = c.Get(1)  // 1 is now most recently used
		c.Put(3, "3") // should evict 2, not 1

		// Cache should only have two entries: a and c.
		assert.Equal(t, c.NumEntries(), 2)
		assert.Equal(t, c.Get(2), nil)
		assert.Equal(t, c.Get(1), "1")
		assert.Equal(t, c.Get(3), "3")
	})
}
