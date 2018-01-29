package kvstore

import (
	"fmt"
	"testing"

	"github.com/savaki/kstreams"
	"github.com/savaki/kstreams/kvstore/memorystore"
	"github.com/tj/assert"
)

func TestAbstractObserver(t *testing.T) {
	observer := AbstractObserver{
		OnPutFunc: func(key, value kstreams.Encoder, err error) {
			fmt.Printf("OnPut(%v, %v) => %v\n", key, value, err)
		},
		OnGetFunc: func(key kstreams.Encoder, value []byte, err error) {
			fmt.Printf("OnGet(%v) => (%v, %v)\n", key, string(value), err)
		},
	}

	var (
		key   = kstreams.StringEncoder("hello")
		value = kstreams.StringEncoder("world")
	)

	store := WithObserver(memorystore.New("blah"), observer)
	assert.Nil(t, store.Put(key, value))

	found, err := store.Get(key)
	assert.Nil(t, err)
	assert.EqualValues(t, value, found)
}
