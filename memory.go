// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
package kstreams

import (
	"sort"
	"sync"
)

// MemoryStore provides an in memory implementation of a KeyValueStore
type MemoryStore struct {
	name    string
	mutex   sync.Mutex
	keys    []string
	content map[string][]byte
}

// remove an element from a slice
func (s *MemoryStore) remove(slice []string, index int) []string {
	return append(slice[:index], slice[index+1:]...)
}

// All provides a closure over all keys and MUST NOT return null values.
//
// No ordering guarantees are provided.
func (s *MemoryStore) All(callback func(key, value []byte) error) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, key := range s.keys {
		if err := callback([]byte(key), s.content[key]); err != nil {
			return err
		}
	}

	return nil
}

// ApproximateNumEntries returns the approximate count of key value
// mappings in the store
func (s *MemoryStore) ApproximateNumEntries() (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return int64(len(s.content)), nil
}

// Delete the value from the store
func (s *MemoryStore) Delete(key Encoder) ([]byte, error) {
	v, err := key.Encode()
	if err != nil {
		return nil, err
	}
	k := string(v)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.content == nil {
		return nil, ErrKeyNotFound
	}

	value, ok := s.content[k]
	if !ok {
		return nil, ErrKeyNotFound
	}

	delete(s.content, k)
	for index, v := range s.keys {
		if v == k {
			s.keys = s.remove(s.keys, index)
			break
		}
	}

	return value, nil
}

// Get the value corresponding to the specified key
func (s *MemoryStore) Get(key Encoder) ([]byte, error) {
	k, err := key.Encode()
	if err != nil {
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.content == nil {
		return nil, ErrKeyNotFound
	}

	v, ok := s.content[string(k)]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return v, nil
}

// put provides an internal implementation of put used by Put, PutIfAbsent,
// and PutAll
func (s *MemoryStore) put(key string, value []byte) {
	if s.content == nil {
		s.content = map[string][]byte{}
	}

	if _, ok := s.content[key]; !ok {
		s.keys = append(s.keys, key)
		sort.Strings(s.keys)
	}

	s.content[key] = value
}

// Put updates the provided key value pair
func (s *MemoryStore) Put(key, value Encoder) error {
	k, err := key.Encode()
	if err != nil {
		return err
	}

	v, err := value.Encode()
	if err != nil {
		return err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.put(string(k), v)

	return nil
}

// PutAll updates all the given key value pairs
func (s *MemoryStore) PutAll(kvs ...KeyValue) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, kv := range kvs {
		k, err := kv.Key.Encode()
		if err != nil {
			return err
		}

		v, err := kv.Value.Encode()
		if err != nil {
			return err
		}

		s.put(string(k), v)
	}

	return nil
}

// PutIfAbsent updates the values associated with this key unless a
// value is already associated with the key
func (s *MemoryStore) PutIfAbsent(key, value Encoder) error {
	k, err := key.Encode()
	if err != nil {
		return err
	}

	v, err := value.Encode()
	if err != nil {
		return err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.content[string(k)]; ok {
		return nil
	}

	s.put(string(k), v)

	return nil
}

// Range over a given set of keys, inclusive.  Range MUST NOT return
// null values.
//
// No ordering guarantees are provided.
func (s *MemoryStore) Range(from, to Encoder, callback func(key, value []byte) error) error {
	f, err := from.Encode()
	if err != nil {
		return err
	}

	t, err := to.Encode()
	if err != nil {
		return err
	}

	fromStr := string(f)
	toStr := string(t)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, key := range s.keys {
		if key < fromStr || key > toStr {
			continue
		}

		if err := callback([]byte(key), s.content[key]); err != nil {
			return err
		}
	}

	return nil
}

// Name of this store
func (s *MemoryStore) Name() string {
	return s.name
}

// IsOpen for reading and writing; always returns true
func (s *MemoryStore) IsOpen() bool {
	return true
}

// IsPersistent indicates if this store is persistent or not; always returns false
func (s *MemoryStore) IsPersistent() bool {
	return false
}

// Flush any cached data; always returns nil
func (s *MemoryStore) Flush() error {
	return nil
}

// Close the storage engine; always returns nil
func (s *MemoryStore) Close() error {
	return nil
}

// NewMemoryStore returns a new in memory key value store with the specified name
func NewMemoryStore(name string) *MemoryStore {
	return &MemoryStore{
		name: name,
	}
}
