package lru_test

import (
	"reflect"
	"testing"

	"github.com/johngibb/fetcher/lru"
)

func TestZeroSizeCache(t *testing.T) {
	c := lru.New(0)
	c.Put(1, "1")
	if n := c.Len(); n != 0 {
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
		assertEqual(t, "c.Len()", c.Len(), 2)
		assertEqual(t, "c.Get(1)", c.Get(1), nil)
		assertEqual(t, "c.Get(2)", c.Get(2), nil)
		assertEqual(t, "c.Get(3)", c.Get(3), "3")
		assertEqual(t, "c.Get(4)", c.Get(4), "4")
	})
	t.Run("get sets last used time", func(t *testing.T) {
		c := lru.New(2)

		c.Put(1, "1")
		c.Put(2, "2")
		_ = c.Get(1)  // 1 is now most recently used
		c.Put(3, "3") // should evict 2, not 1

		// Cache should only have two entries: a and c.
		assertEqual(t, "c.Len()", c.Len(), 2)
		assertEqual(t, "c.Get(2)", c.Get(2), nil)
		assertEqual(t, "c.Get(1)", c.Get(1), "1")
		assertEqual(t, "c.Get(3)", c.Get(3), "3")
	})
}

func assertEqual(t *testing.T, desc string, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: got %v, want %v", desc, got, want)
	}
}
