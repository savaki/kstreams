package store

import (
	"fmt"
	"testing"

	"github.com/savaki/kstreams"
	"github.com/savaki/kstreams/store/memorystore"
	"github.com/tj/assert"
)

func TestAbstractObserver(t *testing.T) {
	observer := AbstractObserver{
		OnPutFunc: func(key kstreams.Encoder, err error) {
			fmt.Println("OnPut")
		},
		OnGetFunc: func(kstreams.Encoder, []byte, error) {
			fmt.Println("OnGet")
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
