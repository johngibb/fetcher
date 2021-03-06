// Package lru implements an LRU cache.
package lru

// Cache implements an LRU cache.
type Cache struct {
	keys map[interface{}]*cacheElem
	head *cacheElem // circular list
	size int        // keys
}

type cacheElem struct {
	prev, next *cacheElem
	key        interface{}
	val        interface{}
}

// New creates a new LRU cache. If size <= 0 the cache is disabled.
func New(size int) *Cache {
	if size <= 0 {
		return &Cache{size: 0}
	}
	head := &cacheElem{}
	head.prev = head
	head.next = head
	return &Cache{
		keys: make(map[interface{}]*cacheElem),
		head: head,
		size: size,
	}
}

func (c *Cache) Put(key, val interface{}) {
	if c.size <= 0 {
		return
	}
	n := len(c.keys)
	if elem := c.keys[key]; elem != nil {
		elem.val = val
		return
	}
	if n >= c.size {
		if x := c.head.prev; x != c.head {
			x.next.prev, x.prev.next = x.prev, x.next
			delete(c.keys, x.key)
		}
	}
	elem := &cacheElem{prev: c.head, next: c.head.next, key: key, val: val}
	c.head.next.prev, c.head.next = elem, elem // insert at front
	c.keys[key] = elem
}

func (c *Cache) Get(key interface{}) interface{} {
	if c.size <= 0 {
		return nil
	}
	elem := c.keys[key]
	if elem == nil {
		return nil
	}
	elem.prev.next, elem.next.prev = elem.next, elem.prev // unlink
	c.head.next.prev, c.head.next = elem, elem            // insert
	return elem.val
}

func (c *Cache) Len() int {
	return len(c.keys)
}
