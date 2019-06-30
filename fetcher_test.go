package fetcher_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johngibb/fetcher"
)

func ExampleFetcher() {
	type Customer struct{ ID int64 }

	// Define a fetcher function. In reality, this would probably hit an
	// external datastore, and have a timeout set.
	fetch := func(ids []int64) (map[int64]interface{}, error) {
		result := make(map[int64]interface{})
		for _, id := range ids {
			result[id] = Customer{ID: id}
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
	if err != nil {
		panic(err)
	}

	fmt.Printf("num returned: %d\n", len(customers))
	fmt.Printf("customer[1].ID: %d\n", customers[1].(Customer).ID)
	// Output:
	// num returned: 2
	// customer[1].ID: 1
}

func TestFetchSuccess(t *testing.T) {
	var fetches [][]int64
	fetch := func(ids []int64) (map[int64]interface{}, error) {
		result := make(map[int64]interface{})
		fetches = append(fetches, ids)
		for _, id := range ids {
			result[id] = fmt.Sprint(id)
		}
		return result, nil
	}
	f := fetcher.New(&fetcher.Config{
		Wait:      1 * time.Millisecond,
		CacheTTL:  100 * time.Second,
		CacheSize: 100,
		BatchSize: 10,
		Fetch:     fetch,
	})

	var wg sync.WaitGroup
	var expectedFetches [][]int64

	// First, submit 2 requests: 1,2,3 and 3,4,5. They should be fetched in one
	// batch.
	expectedFetches = append(expectedFetches, []int64{1, 2, 3, 4, 5})
	wg.Add(2)
	go func() {
		results, err := f.Get(context.Background(), []int64{1, 2, 3})
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("len(results): got %d, want 3", len(results))
		}
		wg.Done()
	}()

	go func() {
		results, err := f.Get(context.Background(), []int64{3, 4, 5})
		if err != nil {
			t.Fatal(err)
		}
		want := map[int64]interface{}{3: "3", 4: "4", 5: "5"}
		if !reflect.DeepEqual(results, want) {
			t.Errorf("results: got %v, want %v", results, want)
		}
		wg.Done()
	}()

	// Now, submit a request for 4,5,6. 4 and 5 should be cached, so only 6
	// should be fetched.
	expectedFetches = append(expectedFetches, []int64{6})
	wg.Wait()
	wg.Add(1)
	go func() {
		results, err := f.Get(context.Background(), []int64{4, 5, 6})
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("len(results): got %d, want 3", len(results))
		}
		wg.Done()
	}()

	// Finally, submit another request for 4,5,6. This should be entirely
	// cached.
	wg.Wait()
	wg.Add(1)
	go func() {
		results, err := f.Get(context.Background(), []int64{4, 5, 6})
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("len(results): got %d, want 3", len(results))
		}
		wg.Done()
	}()

	wg.Wait()

	got, want := normalize(fetches), normalize(expectedFetches)
	if got != want {
		t.Errorf("fetches: got %s, want %s", got, want)
	}
}

func TestFetchError(t *testing.T) {
	fetch := func(ids []int64) (map[int64]interface{}, error) {
		return nil, errors.New("internal error")
	}
	f := fetcher.New(&fetcher.Config{
		Wait:      10 * time.Millisecond,
		CacheTTL:  100 * time.Second,
		CacheSize: 100,
		BatchSize: 10,
		Fetch:     fetch,
	})

	const k = 10 // 10 simultaneous goroutines get an error.
	var wg sync.WaitGroup
	wg.Add(k)
	for i := 0; i < k; i++ {
		go func() {
			defer wg.Done()
			_, err := f.Get(context.Background(), []int64{1, 2, 3})
			want := errors.New("internal error")
			if !reflect.DeepEqual(err, want) {
				t.Errorf("err: got %v, want %v", err, want)
			}
		}()
	}
	wg.Wait()
}

func TestFetchPanic(t *testing.T) {
	fetch := func(ids []int64) (map[int64]interface{}, error) {
		panic("internal error")
	}
	f := fetcher.New(&fetcher.Config{
		Wait:      10 * time.Millisecond,
		CacheTTL:  100 * time.Second,
		CacheSize: 100,
		BatchSize: 10,
		Fetch:     fetch,
	})

	_, err := f.Get(context.Background(), []int64{1, 2, 3})
	if err == nil {
		t.Fatal("no error returned")
	}
	if !strings.Contains(err.Error(), "panic: internal error") {
		t.Errorf("err missing panic message. got:\n%s", err.Error())
	}
	if !strings.Contains(err.Error(), "fetcher.go:") {
		t.Errorf("err missing stack trace. got:\n%s", err.Error())
	}
}

func TestFetchHighLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	fetch := func(ids []int64) (map[int64]interface{}, error) {
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		result := make(map[int64]interface{})
		for _, id := range ids {
			result[id] = fmt.Sprint(id)
		}
		return result, nil
	}

	f := fetcher.New(&fetcher.Config{
		Wait:      1 * time.Millisecond,
		CacheTTL:  100 * time.Second,
		CacheSize: 100,
		BatchSize: 10,
		Fetch:     fetch,
	})

	const k = 1000
	var wg sync.WaitGroup
	wg.Add(k)
	for i := 0; i < k; i++ {
		go func() {
			// Sleep a random amount of time.
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

			// Ask for 100 items from the cache, where ids are a normal
			// distribution between 1 and 1000.
			ids := randomIDs(100, 1000)
			results, err := f.Get(context.Background(), ids)
			if err != nil {
				t.Fatal(err)
			}
			want := expectedResult(ids)
			if !reflect.DeepEqual(want, results) {
				t.Errorf("results: got %v, want %v", results, want)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

// randomIDs returns count random ids in a normal distribution, where each ID is
// in the range [0, max).
func randomIDs(count, max int) []int64 {
	result := make([]int64, count)
	for i := range result {
		result[i] = int64(rand.NormFloat64() * float64(max))
	}
	return result
}

func expectedResult(ids []int64) map[int64]interface{} {
	result := make(map[int64]interface{}, len(ids))
	for _, id := range ids {
		result[id] = fmt.Sprint(id)
	}
	return result
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

func toJSON(val interface{}) string {
	b, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return string(b)
}
