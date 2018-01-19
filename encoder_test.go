package kstreams

import (
	"testing"

	"github.com/tj/assert"
)

func TestStringEncoder(t *testing.T) {
	encoder := StringEncoder("hello")
	data, err := encoder.Encode()
	assert.Nil(t, err)
	assert.Equal(t, "hello", string(data))
}

func TestByteEncoder(t *testing.T) {
	encoder := ByteEncoder("hello")
	data, err := encoder.Encode()
	assert.Nil(t, err)
	assert.Equal(t, "hello", string(data))
}
