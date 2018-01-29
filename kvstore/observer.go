package kvstore

import (
	"github.com/savaki/kstreams"
)

type AbstractObserver struct {
	OnAllFunc                   func(err error)
	OnApproximateNumEntriesFunc func(int64, error)
	OnGetFunc                   func(kstreams.Encoder, []byte, error)
	OnRangeFunc                 func(from, to kstreams.Encoder, err error)
	OnPutFunc                   func(key, value kstreams.Encoder, err error)
	OnPutAllFunc                func(kvs ...kstreams.KeyValue)
	OnPutIfAbsentFunc           func(key, value kstreams.Encoder, err error)
	OnDeleteFunc                func(key kstreams.Encoder, oldValue []byte, err error)
}

func (a AbstractObserver) OnAll(err error) {
	if a.OnAllFunc != nil {
		a.OnAllFunc(err)
	}
}

func (a AbstractObserver) OnApproximateNumEntries(v int64, err error) {
	if a.OnApproximateNumEntriesFunc != nil {
		a.OnApproximateNumEntriesFunc(v, err)
	}
}

func (a AbstractObserver) OnGet(key kstreams.Encoder, value []byte, err error) {
	if a.OnGetFunc != nil {
		a.OnGetFunc(key, value, err)
	}
}

func (a AbstractObserver) OnRange(from, to kstreams.Encoder, err error) {
	if a.OnRangeFunc != nil {
		a.OnRangeFunc(from, to, err)
	}
}
func (a AbstractObserver) OnPut(key, value kstreams.Encoder, err error) {
	if a.OnPutFunc != nil {
		a.OnPutFunc(key, value, err)
	}
}
func (a AbstractObserver) OnPutAll(kvs ...kstreams.KeyValue) {
	if a.OnPutAllFunc != nil {
		a.OnPutAllFunc(kvs...)
	}
}
func (a AbstractObserver) OnPutIfAbsent(key, value kstreams.Encoder, err error) {
	if a.OnPutIfAbsentFunc != nil {
		a.OnPutIfAbsentFunc(key, value, err)
	}
}
func (a AbstractObserver) OnDelete(key kstreams.Encoder, oldValue []byte, err error) {
	if a.OnDeleteFunc != nil {
		a.OnDeleteFunc(key, oldValue, err)
	}
}

type Observer interface {
	OnAll(err error)
	OnApproximateNumEntries(int64, error)
	OnGet(kstreams.Encoder, []byte, error)
	OnRange(from, to kstreams.Encoder, err error)
	OnPut(key, value kstreams.Encoder, err error)
	OnPutAll(kvs ...kstreams.KeyValue)
	OnPutIfAbsent(key, value kstreams.Encoder, err error)
	OnDelete(key kstreams.Encoder, oldValue []byte, err error)
}

type observableKeyValueStore struct {
	store     kstreams.KeyValueStore
	observers []Observer
}

func (o observableKeyValueStore) notify(callback func(observer Observer)) {
	for _, observer := range o.observers {
		callback(observer)
	}
}

// All provides a closure over all keys and MUST NOT return null values.
//
// No ordering guarantees are provided.
func (o observableKeyValueStore) All(callback func(key, value []byte) error) (err error) {
	defer o.notify(func(o Observer) { o.OnAll(err) })
	err = o.store.All(callback)
	return
}

// ApproximateNumEntries returns the approximate count of key value
// mappings in the store
func (o observableKeyValueStore) ApproximateNumEntries() (n int64, err error) {
	defer o.notify(func(o Observer) { o.OnApproximateNumEntries(n, err) })
	n, err = o.store.ApproximateNumEntries()
	return
}

// Get the value corresponding to the specified key
func (o observableKeyValueStore) Get(key kstreams.Encoder) (value []byte, err error) {
	defer o.notify(func(o Observer) { o.OnGet(key, value, err) })
	value, err = o.store.Get(key)
	return
}

// Range over a given set of keys, inclusive.  Range MUST NOT return
// null values.
//
// No ordering guarantees are provided.
func (o observableKeyValueStore) Range(from, to kstreams.Encoder, callback func(key, value []byte) error) (err error) {
	defer o.notify(func(o Observer) { o.OnRange(from, to, err) })
	err = o.store.Range(from, to, callback)
	return
}

// Put updates the provided key value pair
func (o observableKeyValueStore) Put(key, value kstreams.Encoder) (err error) {
	defer o.notify(func(o Observer) { o.OnPut(key, value, err) })
	err = o.store.Put(key, value)
	return
}

// PutAll updates all the given key value pairs
func (o observableKeyValueStore) PutAll(kvs ...kstreams.KeyValue) (err error) {
	defer o.notify(func(o Observer) { o.OnPutAll(kvs...) })
	err = o.store.PutAll(kvs...)
	return
}

// PutIfAbsent updates the values associated with this key unless a
// value is already associated with the key
func (o observableKeyValueStore) PutIfAbsent(key, value kstreams.Encoder) (err error) {
	defer o.notify(func(o Observer) { o.OnPutIfAbsent(key, value, err) })
	err = o.store.PutIfAbsent(key, value)
	return
}

// Delete the value from the store
func (o observableKeyValueStore) Delete(key kstreams.Encoder) (oldValue []byte, err error) {
	defer o.notify(func(o Observer) { o.OnDelete(key, oldValue, err) })
	oldValue, err = o.store.Delete(key)
	return
}

func WithObserver(store kstreams.KeyValueStore, observers ...Observer) kstreams.KeyValueStore {
	return &observableKeyValueStore{
		store:     store,
		observers: observers,
	}
}
