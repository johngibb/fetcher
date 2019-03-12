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
	Fetch func([]int64) (map[int64]interface{}, error)

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
	fetchFn   func([]int64) (map[int64]interface{}, error)
	wait      time.Duration

	batch   *batch     // current pending batch
	cache   *lru.Cache // response cache
	muBatch sync.Mutex // synchronizes access to batch
	muCache sync.Mutex // synchronizes access to cache
}

// batch represents a single fetch.
type batch struct {
	ids     map[int64]struct{} // set of ids in batch
	runOnce sync.Once          // ensure batch is only fetched once
	done    chan struct{}      // signal that batch is done

	results map[int64]interface{} // results from batch
	err     error                 // error from batch
}

func newBatch() *batch {
	return &batch{
		done:    make(chan struct{}),
		ids:     make(map[int64]struct{}),
		results: make(map[int64]interface{}),
	}
}

type cacheEntry struct {
	val     interface{}
	expires time.Time
}

// New creates a new Fetcher with the provided parameters.
func New(cfg *Config) *Fetcher {
	return &Fetcher{
		batchSize: cfg.BatchSize,
		cacheTTL:  cfg.CacheTTL,
		wait:      cfg.Wait,
		fetchFn:   cfg.Fetch,

		batch: newBatch(),
		cache: lru.New(cfg.CacheSize),
	}
}

// submit adds the given ids to the pending batch.
func (f *Fetcher) submit(ids []int64) *batch {
	// Add the ids to the current batch.
	f.muBatch.Lock()
	batch := f.batch
	for _, id := range ids {
		batch.ids[id] = struct{}{}
	}

	// If the batch exceeds the batch size, run it now.
	if len(batch.ids) >= f.batchSize {
		// Create a new batch while still in the critical section.
		f.batch = newBatch()
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
			f.batch = newBatch()
		}
		f.muBatch.Unlock()

		// Reassemble the id set into a slice of ids.
		ids := make([]int64, 0, len(batch.ids))
		for id := range batch.ids {
			ids = append(ids, id)
		}

		// Perform the fetch.
		results, err := f.fetch(ids)

		// Populate the batch results.
		batch.err = err
		for id, r := range results {
			batch.results[id] = r
		}

		// Save this batch to cache.
		f.saveToCache(batch.results)

		// Signal to all waiting callers that the batch is complete.
		close(batch.done)
	})
}

// fetch calls the provided Fetch function, catching panics.
func (f *Fetcher) fetch(ids []int64) (_ map[int64]interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicErr(4, r)
		}
	}()
	return f.fetchFn(ids)
}

func (f *Fetcher) saveToCache(results map[int64]interface{}) {
	if f.cacheTTL == 0 {
		return
	}
	expires := time.Now().Add(f.cacheTTL)
	f.muCache.Lock()
	for id, val := range results {
		f.cache.Put(id, cacheEntry{val, expires})
	}
	f.muCache.Unlock()
}

func (f *Fetcher) getFromCache(ids []int64) (cached map[int64]interface{}, uncached []int64) {
	if f.cacheTTL == 0 {
		return nil, ids
	}
	result := make(map[int64]interface{})
	now := time.Now()
	f.muCache.Lock()
	for _, id := range ids {
		if entry, ok := f.cache.Get(id).(cacheEntry); ok && entry.expires.After(now) {
			result[id] = entry.val
			continue
		}
		uncached = append(uncached, id)
	}
	f.muCache.Unlock()
	return result, uncached
}

// Get fetches all entities for the given ids. It first finds any cached values
// it can. Then, the remaining values are fetched with the next batch.
func (f *Fetcher) Get(ctx context.Context, ids []int64) (map[int64]interface{}, error) {
	cached, uncached := f.getFromCache(ids)

	var batch *batch
	if len(uncached) > 0 {
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

	result := make(map[int64]interface{}, len(ids))
	for _, id := range ids {
		if val, ok := cached[id]; ok {
			result[id] = val
		}
		if batch != nil {
			if val, ok := batch.results[id]; ok {
				result[id] = val
			}
		}
	}
	return result, nil
}
