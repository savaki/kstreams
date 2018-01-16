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

package kstreams

// StateStore provides a storage engine for managing state maintained by a stream processor.
//
// If the store is implemented as a persistent store:
//  * it MUST use the store name as directory name and write all data into this store directory
//  * it MUST create the store directory with the state directory
//  * it MAY obtain the state directory via ProcessorContext.StateDir()
//
// Using nested store directories within the state directory isolates different state stores.
// If a state store would write into the state directory directly, it might conflict with others state stores and thus,
// data might get corrupted and/or Streams might fail with an error.
// Furthermore, Kafka Streams relies on using the store name as store directory name to perform internal cleanup tasks.
//
// This interface does not specify any query capabilities, which, of course,
// would be query engine specific. Instead it just specifies the minimum
// functionality required to reload a storage engine from its changelog as well
// as basic lifecycle management.
type StateStore interface {
	// Close the storage engine
	Close() error

	// Flush any cached data
	Flush() error

	// IsOpen for reading and writing
	IsOpen() bool

	// IsPersistent indicates if this store is persistent or not
	IsPersistent() bool

	// Name of this store
	Name() string
}

// ReadOnlyKeyValueStore represents a key value store that only supports
// read operations.
//
// Implementations should be thread-safe as concurrent reads and writes
// are expected.
type ReadOnlyKeyValueStore interface {
	// All provides a closure over all keys and MUST NOT return null values.
	//
	// No ordering guarantees are provided.
	All(callback func(key, value []byte) error) error

	// ApproximateNumEntries returns the approximate count of key value
	// mappings in the store
	ApproximateNumEntries() (int64, error)

	// Get the value corresponding to the specified key
	Get(key Encoder) ([]byte, error)

	// Range over a given set of keys, inclusive.  Range MUST NOT return
	// null values.
	//
	// No ordering guarantees are provided.
	Range(from, to Encoder, callback func(key, value []byte) error) error
}

type KeyValue struct {
	Key   Encoder
	Value Encoder
}

// KeyValueStore represents a key value store that supports put/get/delete
// and range queries
type KeyValueStore interface {
	ReadOnlyKeyValueStore

	// Put updates the provided key value pair
	Put(key, value Encoder) error

	// PutAll updates all the given key value pairs
	PutAll(kvs ...KeyValue) error

	// PutIfAbsent updates the values associated with this key unless a
	// value is already associated with the key
	PutIfAbsent(key, value Encoder) error

	// Delete the value from the store
	Delete(key Encoder) ([]byte, error)
}
