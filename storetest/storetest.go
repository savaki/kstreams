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

package storetest

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/savaki/kstreams"
	"github.com/tj/assert"
)

const (
	letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func alphaN(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Int63()%int64(len(letters))]
	}
	return string(b)
}

type encoder string

func (e encoder) Encode() ([]byte, error) {
	return []byte(e), nil
}

func TestStore(t *testing.T, store kstreams.KeyValueStore) {
	t.Run("get nonexistent key", func(t *testing.T) {
		key := encoder(alphaN(8))
		_, err := store.Get(key)
		assert.Equal(t, kstreams.ErrKeyNotFound, err)
	})

	t.Run("put then get", func(t *testing.T) {
		key := encoder(alphaN(8))
		value := encoder("world")
		assert.Nil(t, store.Put(key, value))

		data, err := store.Get(key)
		assert.Nil(t, err)
		assert.EqualValues(t, value, encoder(data))
	})

	t.Run("put if absent doesn't overwrite value, then get", func(t *testing.T) {
		key := encoder(alphaN(8))
		value1 := encoder("world1")
		value2 := encoder("world2")
		assert.Nil(t, store.Put(key, value1))
		assert.Nil(t, store.PutIfAbsent(key, value2))

		data, err := store.Get(key)
		assert.Nil(t, err)
		assert.EqualValues(t, value1, encoder(data))
	})

	t.Run("put if absent, then get", func(t *testing.T) {
		key := encoder(alphaN(8))
		value := encoder("world1")
		assert.Nil(t, store.PutIfAbsent(key, value))

		data, err := store.Get(key)
		assert.Nil(t, err)
		assert.EqualValues(t, value, encoder(data))
	})

	t.Run("put all followed by get range and all", func(t *testing.T) {
		key := alphaN(10)
		key1 := encoder(key + "1")
		key2 := encoder(key + "2")
		key3 := encoder(key + "3")
		value := encoder("blah")

		err := store.PutAll(
			kstreams.KeyValue{
				Key:   key1,
				Value: value,
			},
			kstreams.KeyValue{
				Key:   key2,
				Value: value,
			},
			kstreams.KeyValue{
				Key:   key3,
				Value: value,
			},
		)
		assert.Nil(t, err)

		t.Run("and retrieved via Range", func(t *testing.T) {
			count := 0
			store.Range(key1, key3, func(k, v []byte) error {
				count++
				switch count {
				case 1:
					assert.Equal(t, key1, encoder(k))
				case 2:
					assert.Equal(t, key2, encoder(k))
				case 3:
					assert.Equal(t, key3, encoder(k))
				}
				assert.Equal(t, value, encoder(v))
				return nil
			})
			assert.Equal(t, 3, count)
		})

		t.Run("and retrieved via All", func(t *testing.T) {
			counts := map[int]struct{}{}
			store.All(func(k, v []byte) error {
				if key := encoder(k); reflect.DeepEqual(key, key1) {
					counts[1] = struct{}{}

				} else if reflect.DeepEqual(key, key2) {
					counts[2] = struct{}{}

				} else if reflect.DeepEqual(key, key3) {
					counts[3] = struct{}{}
				}
				return nil
			})
			assert.Equal(t, 3, len(counts))
		})
	})

	t.Run("put one, get range with keys that don't exist", func(t *testing.T) {
		key := alphaN(12)
		key0 := encoder(key + "0")
		key1 := encoder(key + "1")
		key2 := encoder(key + "2")
		value := encoder("blah")

		err := store.PutAll(
			kstreams.KeyValue{
				Key:   key1,
				Value: value,
			},
		)
		assert.Nil(t, err)

		count := 0
		store.Range(key0, key2, func(k, v []byte) error {
			count++
			return nil
		})
		assert.Equal(t, 1, count)
	})

	t.Run("delete", func(t *testing.T) {
		key := encoder(alphaN(8))
		value := encoder("world1")

		assert.Nil(t, store.Put(key, value))

		found, err := store.Delete(key)
		assert.Nil(t, err)
		assert.Equal(t, value, encoder(found))

		_, err = store.Get(key)
		assert.Equal(t, kstreams.ErrKeyNotFound, err)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		key := encoder(alphaN(10))
		_, err := store.Delete(key)
		assert.Equal(t, kstreams.ErrKeyNotFound, err)
	})

	t.Run("implements StateStore", func(t *testing.T) {
		stateStore, ok := store.(kstreams.StateStore)
		assert.True(t, ok, "expected store to implement kstreams.StateStore")

		t.Run("and name is set", func(t *testing.T) {
			assert.NotZero(t, stateStore.Name())
		})

		t.Run("and IsOpen", func(t *testing.T) {
			assert.True(t, stateStore.IsOpen())
		})
	})
}
