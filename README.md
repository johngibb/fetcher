# Fetcher

[![GoDoc](https://godoc.org/github.com/johngibb/fetcher?status.svg)][godoc]

Fetcher is a library for batching and caching concurrent requests for entities
by their identifiers.

## Example

```go
fetch := func(ids []int64) (map[int64]*Customer{}, error) {
    // Create a context with a 10 second timeout.
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    // Make a database query to fetch this batch of customers.
    return repo.GetCustomers(ctx, ids)
}

// Create a new fetcher.
f := fetcher.New(&fetcher.Config{
    Wait:      1 * time.Millisecond,
    BatchSize: 10,
    Fetch:     fetch,
    CacheTTL:  100 * time.Second,
    CacheSize: 100,
})

// Fetch customers 1 and 2. If another goroutine asks for customers at the same
// time, they will all be fetched in the same batch. In addition, some customers
// may be returned from the cache.
customers, err := f.Get(context.Background(), []int64{1, 2})
```

For more information, please see the [docs][godoc].

[godoc]: https://godoc.org/github.com/johngibb/fetcher
