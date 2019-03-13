# Fetcher

[![GoDoc](https://godoc.org/github.com/johngibb/fetcher?status.svg)][godoc]

Fetcher is a library for batching and caching concurrent requests for entities
by their identifiers.

## Example

```go
fetch := func(ids []int64) (map[int64]interface{}, error) {
    // Create a context with a 10 second timeout.
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    // Make a database query to fetch this batch of customers.
    customers, err := repo.GetCustomers(ctx, ids)
    if err != nil {
        return nil, err
    }

    // Return the result as a map[int64]interface{}.
    result := make(map[int64]interface{})
    for id, customer := range customers {
        result[id] = customer
    }
    return result, nil
}

// Create a new fetcher.
f := fetcher.New(&fetcher.Config{
    Wait:      1 * time.Millisecond,
    BatchSize: 10,
    Fetch:     fetch,
    CacheTTL:  100 * time.Second,
    CacheSize: 100,
})

// Fetch customers 1 and 2.
customers, err := f.Get(context.Background(), []int64{1, 2})
```

For more information, please see the [docs][godoc].

[godoc]: https://godoc.org/github.com/johngibb/fetcher
