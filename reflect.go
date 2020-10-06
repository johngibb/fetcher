package fetcher

import (
	"errors"
	"reflect"
)

var invalidSignature = errors.New("func should be func([]KeyType) (map[KeyType]ValueType, error)")

type fetchFunc struct {
	keyType, valType reflect.Type
	fn               reflect.Value
}

func newFetchFunc(fn interface{}) fetchFunc {
	t := reflect.TypeOf(fn)
	if t.Kind() != reflect.Func {
		panic(invalidSignature)
	}
	if t.NumIn() != 1 || t.NumOut() != 2 {
		panic(invalidSignature)
	}
	var (
		arg1 = t.In(0)
		out1 = t.Out(0)
		out2 = t.Out(1)
	)
	if out2 != reflect.TypeOf((*error)(nil)).Elem() {
		panic(invalidSignature)
	}
	if arg1.Kind() != reflect.Slice {
		panic(invalidSignature)
	}
	keyType := arg1.Elem()

	if out1.Kind() != reflect.Map {
		panic(invalidSignature)
	}
	if out1.Key() != keyType {
		panic(invalidSignature)
	}
	valType := out1.Elem()
	return fetchFunc{
		keyType: keyType,
		valType: valType,
		fn:      reflect.ValueOf(fn),
	}
}

func (f fetchFunc) Fetch(ids reflect.Value) (resultMap, error) {
	result := f.fn.Call([]reflect.Value{ids})
	var err error
	if out1 := result[1].Interface(); out1 != nil {
		err = out1.(error)
	}
	return resultMap{result[0]}, err
}

type resultMap struct {
	val reflect.Value
}

func newResultMap(keyType, valType reflect.Type, size int) resultMap {
	mapType := reflect.MapOf(
		keyType,
		valType,
	)
	return resultMap{reflect.MakeMapWithSize(mapType, size)}
}

func (r resultMap) Range() *reflect.MapIter {
	return r.val.MapRange()
}

func (r resultMap) Set(key, val reflect.Value) {
	r.val.SetMapIndex(key, val)
}

func (r resultMap) Get(key reflect.Value) reflect.Value {
	return r.val.MapIndex(key)
}

func (r resultMap) Interface() interface{} {
	return r.val.Interface()
}

var emptyStructVal = reflect.ValueOf(struct{}{})

type keySet struct {
	keyType reflect.Type
	val     reflect.Value
}

func newKeySet(keyType reflect.Type, size int) keySet {
	mapType := reflect.MapOf(
		keyType,
		emptyStructVal.Type(),
	)
	return keySet{keyType, reflect.MakeMapWithSize(mapType, size)}
}

func newKeySetFromSlice(vals interface{}) keySet {
	val := reflect.ValueOf(vals)
	if val.Kind() != reflect.Slice {
		panic("vals was " + val.Type().String() + ", not slice")
	}
	keyType := val.Type().Elem()
	mapType := reflect.MapOf(
		keyType,
		emptyStructVal.Type(),
	)
	s := keySet{keyType, reflect.MakeMapWithSize(mapType, val.Len())}
	for i := 0; i < val.Len(); i++ {
		s.Add(val.Index(i))
	}
	return s
}

func (s keySet) Add(key reflect.Value) {
	s.val.SetMapIndex(key, emptyStructVal)
}

func (s keySet) Contains(key reflect.Value) bool {
	return s.val.MapIndex(key).Kind() != reflect.Invalid
}

func (s keySet) Range() *reflect.MapIter {
	return s.val.MapRange()
}

func (s keySet) Slice() reflect.Value {
	result := reflect.MakeSlice(reflect.SliceOf(s.keyType), 0, s.val.Len())
	iter := s.val.MapRange()
	for iter.Next() {
		result = reflect.Append(result, iter.Key())
	}
	return result
}

func (s keySet) Len() int {
	return s.val.Len()
}
