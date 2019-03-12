package fetcher_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/deliveroo/assert-go"
	"github.com/johngibb/fetcher"
)

func TestDataLoader(t *testing.T) {
	if testing.Short() {
		return
	}

	var fetches [][]int64
	fetch := func(ids []int64) ([]interface{}, error) {
		var result []interface{}
		fetches = append(fetches, ids)
		for _, id := range ids {
			result = append(result, fmt.Sprint(id))
		}
		return result, nil
	}
	f := fetcher.New(&fetcher.Config{
		Wait:      100 * time.Millisecond,
		CacheTTL:  100 * time.Second,
		CacheSize: 100,
		BatchSize: 10,
		Fetch:     fetch,
	})

	var wg sync.WaitGroup
	var expectedFetches [][]int64

	go func() {
		time.Sleep(1 * time.Second)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		panic("not done")
	}()

	// First, submit 2 requests: 1,2,3 and 3,4,5. They should be fetched in one
	// batch.
	expectedFetches = append(expectedFetches, []int64{1, 2, 3, 4, 5})
	wg.Add(2)
	go func() {
		results, err := f.Get(context.Background(), []int64{1, 2, 3})
		assert.Must(t, err)
		assert.Equal(t, len(results), 3)
		wg.Done()
	}()

	go func() {
		results, err := f.Get(context.Background(), []int64{3, 4, 5})
		assert.Must(t, err)
		assert.Equal(t, len(results), 3)
		wg.Done()
	}()

	// Now, submit a request for 4,5,6. 4 and 5 should be cached, so only 6
	// should be fetched.
	expectedFetches = append(expectedFetches, []int64{6})
	wg.Wait()
	wg.Add(1)
	go func() {
		results, err := f.Get(context.Background(), []int64{4, 5, 6})
		assert.Must(t, err)
		assert.Equal(t, len(results), 3)
		wg.Done()
	}()

	// Finally, submit another request for 4,5,6. This should be entirely
	// cached.
	wg.Wait()
	wg.Add(1)
	go func() {
		results, err := f.Get(context.Background(), []int64{4, 5, 6})
		assert.Must(t, err)
		assert.Equal(t, len(results), 3)
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, normalize(fetches), normalize(expectedFetches))
}

// normalize sorts the input and marshals it to JSON for easy comparison.
func normalize(fetches [][]int64) string {
	// Sort fetches first.
	for i := range fetches {
		sort.Slice(fetches[i], func(a, b int) bool {
			return fetches[i][a] < fetches[i][b]
		})
	}
	sort.Slice(fetches, func(i, j int) bool {
		return fetches[i][0] < fetches[j][0]
	})

	b, err := json.Marshal(fetches)
	if err != nil {
		panic(err)
	}
	return string(b)
}
