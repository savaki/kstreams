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

package badgerstore

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	"github.com/savaki/kstreams"
)

// Store implements a kstreams.KeyValueStore backed by badger
type Store struct {
	name string
	db   *badger.DB

	mutex  sync.Mutex
	closed bool
	err    error
}

// Flush any cached data
func (s *Store) Flush() error {
	return nil
}

// IsOpen for reading and writing
func (s *Store) IsOpen() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return !s.closed
}

// IsPersistent indicates if this store is persistent or not
func (s *Store) IsPersistent() bool {
	return true
}

// Name of this store
func (s *Store) Name() string {
	return s.name
}

// Close the storage engine
func (s *Store) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return s.err
	}

	s.closed = true
	s.err = s.db.Close()

	return s.err
}

// Get the value corresponding to the specified key
func (s *Store) Get(key kstreams.Encoder) ([]byte, error) {
	k, err := key.Encode()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to encode key")
	}

	var value []byte
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err == nil {
			value, err = item.Value()
		}
		return err
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, kstreams.ErrKeyNotFound
		}
		return nil, errors.Wrapf(err, "unable to get key, %v", string(k))
	}

	return value, nil
}

func (s *Store) doRange(from, to []byte, callback func(key, value []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   1e3,
		})

		iter.Seek(from)

		for {
			if !iter.Valid() {
				return nil
			}

			item := iter.Item()
			value, err := item.Value()
			if err != nil {
				return err
			}

			if to != nil {
				if bytes.Compare(item.Key(), to) > 0 {
					return nil
				}
			}

			if err := callback(item.Key(), value); err != nil {
				return err
			}

			iter.Next()
		}
	})
}

// Range over a given set of keys, inclusive.  Range MUST NOT return
// null values.
//
// No ordering guarantees are provided.
func (s *Store) Range(from, to kstreams.Encoder, callback func(key, value []byte) error) error {
	f, err := from.Encode()
	if err != nil {
		return errors.Wrapf(err, "unable to encode key")
	}

	t, err := to.Encode()
	if err != nil {
		return errors.Wrapf(err, "unable to encode key")
	}

	if err := s.doRange(f, t, callback); err != nil {
		return errors.Wrapf(err, "Range failed")
	}

	return nil
}

// All provides a closure over all keys and MUST NOT return null values.
//
// No ordering guarantees are provided.
func (s *Store) All(callback func(key, value []byte) error) error {
	if err := s.doRange(nil, nil, callback); err != nil {
		return errors.Wrapf(err, "Range failed")
	}

	return nil
}

// Put updates the provided key value pair
func (s *Store) Put(key, value kstreams.Encoder) error {
	k, err := key.Encode()
	if err != nil {
		return errors.Wrapf(err, "unable to encode key")
	}

	v, err := value.Encode()
	if err != nil {
		return errors.Wrapf(err, "unable to encode value")
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
	if err != nil {
		return errors.Wrapf(err, "unable to put key, %v", string(k))
	}

	return nil
}

// PutIfAbsent updates the values associated with this key unless a
// value is already associated with the key
func (s *Store) PutIfAbsent(key, value kstreams.Encoder) error {
	k, err := key.Encode()
	if err != nil {
		return errors.Wrapf(err, "unable to encode key")
	}

	v, err := value.Encode()
	if err != nil {
		return errors.Wrapf(err, "unable to encode value")
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		switch _, err := txn.Get(k); err {
		case nil:
			return nil
		case badger.ErrKeyNotFound:
			return txn.Set(k, v)
		default:
			return err
		}
	})
	if err != nil {
		return errors.Wrapf(err, "unable to put key, %v", string(k))
	}

	return nil
}

// PutAll updates all the given key value pairs
func (s *Store) PutAll(kvs ...kstreams.KeyValue) error {
	length := len(kvs)
	if length == 0 {
		return nil
	}

	keys := make([][]byte, length)
	values := make([][]byte, length)
	for index, kv := range kvs {
		k, err := kv.Key.Encode()
		if err != nil {
			return errors.Wrapf(err, "unable to encode key")
		}

		v, err := kv.Value.Encode()
		if err != nil {
			return errors.Wrapf(err, "unable to encode value")
		}

		keys[index] = k
		values[index] = v
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		for i := 0; i < length; i++ {
			if err := txn.Set(keys[i], values[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "PutAll failed")
	}

	return nil
}

// Delete the value from the store
func (s *Store) Delete(key kstreams.Encoder) ([]byte, error) {
	k, err := key.Encode()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to encode key")
	}

	var oldValue []byte
	err = s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			if err != badger.ErrKeyNotFound {
				return err
			}
			return err
		}
		if oldValue, err = item.Value(); err != nil {
			return err
		}

		return txn.Delete(k)
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, kstreams.ErrKeyNotFound
		}
		return nil, errors.Wrapf(err, "Delete failed for key, %v", string(k))
	}

	return oldValue, nil
}

// ApproximateNumEntries returns the approximate count of key value
// mappings in the store
func (s *Store) ApproximateNumEntries() (int64, error) {
	return 0, errors.New("ApproximateNumEntries not supported")
}

// New returns a new badger KeyValueStore that stores data in the specified directory
func New(name, dir string) (*Store, error) {
	path, err := filepath.Abs(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to calculate abs path for %v", dir)
	}

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, errors.Wrapf(err, "unable to create dir path, %v", path)
	}

	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path

	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open badger db, %v", dir)
	}

	return &Store{
		name: name,
		db:   db,
	}, nil
}
