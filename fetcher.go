// Package fetcher handles batching and caching of concurrent fetches.
//
// Fetcher allows the user to specify a function for fetching a batch of
// entities by their identifiers. Then, the user can call fetcher.Get
// concurrently, and the fetcher will coordinate batching multiple requests, and
// caching results.
package fetcher

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/johngibb/fetcher/lru"
)

// PanicError represents a panic that occurred while executing the provided
// fetch function.
type PanicError struct {
	// Panic is the raw panic that occurred.
	Panic interface{}

	callers []uintptr // stack
}

// Error returns the panic and stacktrace.
func (p PanicError) Error() string {
	var msg strings.Builder
	fmt.Fprintf(&msg, "panic: %v\n", p.Panic)
	frames := runtime.CallersFrames(p.callers)
	for {
		frame, more := frames.Next()
		fmt.Fprintf(&msg, "%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
		if !more {
			break
		}
	}
	return msg.String()
}

func newPanicErr(skip int, val interface{}) error {
	const depth = 32
	var pc [32]uintptr
	n := runtime.Callers(skip+1, pc[:])
	return PanicError{val, pc[:n]}
}

// Config is the configuration required to create a fetcher.
type Config struct {
	// Wait is the maximum time to wait for a batch to fill up before running it
	// immediately.
	Wait time.Duration

	// BatchSize is the size at which, once exceeded, the batch will be fetched
	// immediately. Note that the batch of ids passed to Fetch may exceed this
	// size.
	BatchSize int

	// Fetch is the function called to fetch a batch of results. If a panic
	// occurs, it will be handled and returned wrapped in PanicError. However,
	// you may want to add your own panic handling in order to capture a stack
	// trace.
	//
	// Signature: func([]KeyType) (map[KeyType]ValueType, error)
	Fetch interface{}

	// CacheTTL is the amount of time for which response should be cached. Zero
	// will disable caching entirely.
	CacheTTL time.Duration

	// CacheSize bounds the number of entries in the LRU cache.
	CacheSize int
}

// Fetcher caches and batches requests.
type Fetcher struct {
	batchSize int
	cacheTTL  time.Duration
	fetchFn   fetchFunc
	wait      time.Duration

	batch   *batch     // current pending batch
	cache   *lru.Cache // response cache
	muBatch sync.Mutex // synchronizes access to batch
	muCache sync.Mutex // synchronizes access to cache
}

// batch represents a single fetch.
type batch struct {
	ids     keySet        // set of ids in batch
	runOnce sync.Once     // ensure batch is only fetched once
	done    chan struct{} // signal that batch is done

	results resultMap // results from batch
	err     error     // error from batch
}

func newBatch(keyType, valType reflect.Type) *batch {
	return &batch{
		done:    make(chan struct{}),
		ids:     newKeySet(keyType, 0),
		results: newResultMap(keyType, valType, 0),
	}
}

type cacheEntry struct {
	val     interface{}
	expires time.Time
}

// New creates a new Fetcher with the provided parameters.
func New(cfg *Config) *Fetcher {
	fetchFn := newFetchFunc(cfg.Fetch)
	return &Fetcher{
		batchSize: cfg.BatchSize,
		cacheTTL:  cfg.CacheTTL,
		wait:      cfg.Wait,
		fetchFn:   fetchFn,

		batch: newBatch(fetchFn.keyType, fetchFn.valType),
		cache: lru.New(cfg.CacheSize),
	}
}

// submit adds the given ids to the pending batch.
func (f *Fetcher) submit(ids keySet) *batch {
	// Add the ids to the current batch.
	f.muBatch.Lock()
	batch := f.batch
	iter := ids.Range()
	for iter.Next() {
		batch.ids.Add(iter.Key())
	}

	// If the batch exceeds the batch size, run it now.
	if batch.ids.Len() >= f.batchSize {
		// Create a new batch while still in the critical section.
		f.batch = newBatch(f.fetchFn.keyType, f.fetchFn.valType)
		f.muBatch.Unlock()

		// Run the full batch.
		f.run(batch)
		return batch
	}
	f.muBatch.Unlock()

	// Otherwise, start a timer to run it after f.wait.
	go func() {
		<-time.After(f.wait)
		f.run(batch)
	}()

	return batch
}

// run executes the batch, unless it's already been executed.
func (f *Fetcher) run(batch *batch) {
	batch.runOnce.Do(func() {
		// If this batch is still the current batch, start a new one.
		f.muBatch.Lock()
		if f.batch == batch {
			f.batch = newBatch(f.fetchFn.keyType, f.fetchFn.valType)
		}
		f.muBatch.Unlock()

		// Reassemble the id set into a slice of ids.
		ids := newKeySet(f.fetchFn.keyType, batch.ids.Len())

		iter := batch.ids.Range()
		for iter.Next() {
			ids.Add(iter.Key())
		}

		// Perform the fetch.
		results, err := f.fetch(ids)

		// Populate the batch results.
		batch.err = err
		if err == nil {
			iter := results.Range()
			for iter.Next() {
				batch.results.Set(iter.Key(), iter.Value())
			}

			// Save this batch to cache.
			f.saveToCache(batch.results)
		}

		// Signal to all waiting callers that the batch is complete.
		close(batch.done)
	})
}

// fetch calls the provided Fetch function, catching panics.
func (f *Fetcher) fetch(ids keySet) (_ resultMap, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicErr(4, r)
		}
	}()
	return f.fetchFn.Fetch(ids.Slice())
}

func (f *Fetcher) saveToCache(results resultMap) {
	if f.cacheTTL == 0 {
		return
	}
	expires := time.Now().Add(f.cacheTTL)
	f.muCache.Lock()
	iter := results.Range()
	for iter.Next() {
		f.cache.Put(iter.Key().Interface(), cacheEntry{iter.Value(), expires})
	}
	f.muCache.Unlock()
}

func (f *Fetcher) getFromCache(ids keySet) (cached resultMap, uncached keySet) {
	if f.cacheTTL == 0 {
		return newResultMap(f.fetchFn.keyType, f.fetchFn.valType, 0), ids
	}
	uncached = newKeySet(f.fetchFn.keyType, 0)
	result := newResultMap(f.fetchFn.keyType, f.fetchFn.valType, 0)
	now := time.Now()
	f.muCache.Lock()
	iter := ids.Range()
	for iter.Next() {
		if entry, ok := f.cache.Get(iter.Key().Interface()).(cacheEntry); ok && entry.expires.After(now) {
			result.Set(iter.Key(), entry.val.(reflect.Value))
			continue
		}
		uncached.Add(iter.Key())
	}
	f.muCache.Unlock()
	return result, uncached
}

// Get fetches all entities for the given ids. It first finds any cached values
// it can. Then, the remaining values are fetched with the next batch.
func (f *Fetcher) Get(ctx context.Context, ids interface{}) (interface{}, error) {
	idSet := newKeySetFromSlice(ids)

	cached, uncached := f.getFromCache(idSet)

	var batch *batch
	if uncached.Len() > 0 {
		// Submit the ids to the current batch.
		batch = f.submit(uncached)

		// Wait for batch to finish.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-batch.done:
		}
		if batch.err != nil {
			return nil, batch.err
		}
	}

	result := newResultMap(f.fetchFn.keyType, f.fetchFn.valType, idSet.Len())
	iter := idSet.Range()
	for iter.Next() {
		if val := cached.Get(iter.Key()); val.Kind() != reflect.Invalid {
			result.Set(iter.Key(), val)
		}
		if batch != nil {
			if val := batch.results.Get(iter.Key()); val.Kind() != reflect.Invalid {
				result.Set(iter.Key(), val)
			}
		}
	}
	return result.Interface(), nil
}
