package fetcher

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestReflectFunc(t *testing.T) {
	type Person struct{}
	t.Run("validate signature", func(t *testing.T) {
		tests := []struct {
			valid bool
			fn    interface{}
		}{
			// Valid Functions:
			{true, func(ids []int64) (map[int64]*Person, error) { return nil, nil }},
			{true, func(ids []int) (map[int]*Person, error) { return nil, nil }},
			{true, func(ids []int) (map[int]Person, error) { return nil, nil }},

			// Invalid Functions:
			{false, func(ids []int) ([]Person, error) { return nil, nil }},
			{false, func(ctx context.Context, ids []int) (map[int64]Person, error) { return nil, nil }},
			{false, func(ids int) ([]Person, error) { return nil, nil }},
			{false, func(ids []int) (Person, error) { return Person{}, nil }},
			{false, func(ids []int) []Person { return nil }},
		}
		for _, tt := range tests {
			got := true
			(func() {
				defer func() {
					if r := recover(); r != nil {
						got = false
					}
				}()
				_ = newFetchFunc(tt.fn)
			})()
			if got != tt.valid {
				t.Errorf("%v valid: got %v, want %v", reflect.TypeOf(tt.fn), got, tt.valid)
			}
		}
	})

	t.Run("fetch", func(t *testing.T) {
		t.Run("no error", func(t *testing.T) {
			want := map[int]*Person{1: &Person{}}
			fn := func(ids []int) (map[int]*Person, error) {
				return want, nil
			}
			result, err := newFetchFunc(fn).Fetch(reflect.ValueOf([]int{1}))
			if !reflect.DeepEqual(result.Interface(), want) {
				t.Errorf("result: got %v, want %v", result.Interface(), want)
			}
			if err != nil {
				t.Errorf("err: got %v, want nil", err)
			}
		})
		t.Run("error", func(t *testing.T) {
			wantErr := errors.New("error")
			fn := func(ids []int) (map[int]*Person, error) {
				return nil, wantErr
			}
			result, err := newFetchFunc(fn).Fetch(reflect.ValueOf([]int{1}))
			if result.Interface().(map[int]*Person) != nil {
				t.Errorf("result: got %v, want nil", result.Interface())
			}
			if err != wantErr {
				t.Errorf("err: got %v, want %v", err, wantErr)
			}
		})
	})
}

func TestReflectKeySet(t *testing.T) {
	key := reflect.ValueOf(3)

	m := newKeySet(key.Type(), 0)
	assertEQ(t, m.Contains(key), "m.Contains(key)", false)

	m.Add(key)
	assertEQ(t, m.Contains(key), "m.Contains(key)", true)
}

func assertEQ(t *testing.T, got interface{}, msg string, want interface{}) {
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: got %v, want %v", msg, got, want)
	}
}
