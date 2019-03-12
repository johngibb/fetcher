package fetcher

import (
	"context"
	"sync"
	"time"

	"github.com/johngibb/fetcher/lru"
)

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

type Config struct {
	Wait      time.Duration                        // max time to wait for batch to fill up
	BatchSize int                                  // once batch exceeds this size, it's always run
	Fetch     func([]int64) ([]interface{}, error) // fetch function
	CacheTTL  time.Duration                        // cache TTL; zero disables caching.
	CacheSize int                                  // max entries in LRU cache
}

// Fetcher caches and batches requests.
type Fetcher struct {
	wait      time.Duration
	batchSize int
	fetch     func([]int64) ([]interface{}, error)
	cacheTTL  time.Duration

	batch   *batch       // current pending batch
	cache   *lru.Cache   // response cache
	muBatch sync.Mutex   // synchronizes access to batch
	muCache sync.RWMutex // synchronizes access to cache
}

func New(cfg *Config) *Fetcher {
	return &Fetcher{
		batchSize: cfg.BatchSize,
		cacheTTL:  cfg.CacheTTL,
		wait:      cfg.Wait,
		fetch:     cfg.Fetch,

		batch: newBatch(),
		cache: lru.New(cfg.CacheSize),
	}
}

func (f *Fetcher) submit(ids []int64) *batch {
	// Add the ids to the current batch.
	f.muBatch.Lock()
	batch := f.batch
	for _, id := range ids {
		batch.ids[id] = struct{}{}
	}
	currentBatchSize := len(batch.ids)
	f.muBatch.Unlock()

	// If the batch exceeds the batch size, run it now.
	if currentBatchSize >= f.batchSize {
		f.run(batch)
		return batch
	}

	// Otherwise, start a timer to run it after f.wait.
	go func() {
		<-time.After(f.wait)
		f.run(batch)
	}()

	return batch
}

func (f *Fetcher) run(batch *batch) {
	batch.runOnce.Do(func() {
		// Start a new batch for the next request.
		f.muBatch.Lock()
		f.batch = newBatch()
		f.muBatch.Unlock()

		// Reassemble the id set into a slice of ids.
		ids := make([]int64, 0, len(batch.ids))
		for id, _ := range batch.ids {
			ids = append(ids, id)
		}

		// Perform the fetch.
		results, err := f.fetch(ids)

		// Populate the batch results.
		batch.err = err
		for i, r := range results {
			batch.results[ids[i]] = r
		}

		// Save this batch to cache.
		f.saveToCache(batch.results)

		// Signal to all waiting callers that the batch is complete.
		close(batch.done)
	})
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
	f.muCache.RLock()
	for _, id := range ids {
		if entry, ok := f.cache.Get(id).(cacheEntry); ok && entry.expires.After(now) {
			result[id] = entry.val
			continue
		}
		uncached = append(uncached, id)
	}
	f.muCache.RUnlock()
	return result, uncached
}

// Get fetches all entities for the given ids. It first finds any cached values
// it can. Then, the remaining values are fetched with the next batch.
func (f *Fetcher) Get(ctx context.Context, ids []int64) ([]interface{}, error) {
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

	results := make([]interface{}, len(ids))
	for i, id := range ids {
		if val := cached[id]; val != nil {
			results[i] = val
		} else if batch != nil {
			results[i] = batch.results[id]
		}
	}
	return results, nil
}
